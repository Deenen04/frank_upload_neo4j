from neo4j import AsyncGraphDatabase
import logging
from collections import defaultdict

class AsyncNeo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password), max_connection_pool_size=100)
    
    async def close(self):
        await self.driver.close()
    
    async def get_existing_doc_ids_async(self):
        async with self.driver.session() as session:
            result = await session.run("MATCH (doc:Document) RETURN doc.doc_id AS doc_id")
            doc_ids = [record["doc_id"] async for record in result]
            return doc_ids
    
    async def create_document_node_async(self, doc_id, doc_name, doc_date):
        async with self.driver.session() as session:
            await session.run(
                """
                MERGE (doc:Document {doc_id: $doc_id})
                ON CREATE SET doc.name = $doc_name, doc.date = $doc_date
                """,
                doc_id=doc_id, doc_name=doc_name, doc_date=doc_date
            )
            logging.info(f"Document node created or merged with ID: {doc_id}, Name: {doc_name}, Date: {doc_date}")
    
    async def batch_create_chunk_nodes_async(self, doc_id, chunks, file_name, metadata):
        async with self.driver.session() as session:
            await session.write_transaction(
                self._batch_create_chunk_nodes_tx_async, doc_id, chunks, file_name, metadata
            )
    
    @staticmethod
    async def _batch_create_chunk_nodes_tx_async(tx, doc_id, chunks, file_name, metadata):
        params = [
            {
                "doc_id": doc_id,
                "chunk_id": f"{doc_id}_chunk_{i+1}",
                "chunk_text": chunk,
                "chunk_index": metadata[i]['chunk_index'],
                "file_name": file_name
            }
            for i, chunk in enumerate(chunks)
        ]
        await tx.run(
            """
            MATCH (doc:Document {doc_id: $doc_id})
            WITH doc, $params as rows
            UNWIND rows as row
            MERGE (chunk:Chunk {chunk_id: row.chunk_id})
            SET chunk.content = row.chunk_text,
                chunk.chunk_index = row.chunk_index,
                chunk.file_name = row.file_name,
                chunk.doc_id = row.doc_id
            MERGE (doc)-[:CONTAINS]->(chunk)
            """,
            doc_id=doc_id,
            params=params
        )
        logging.info(f"Batch created {len(chunks)} chunk nodes for document {doc_id}.")
    
    async def get_chunks_by_ids_async(self, chunk_ids):
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (chunk:Chunk)
                WHERE chunk.chunk_id IN $chunk_ids
                RETURN chunk.chunk_id AS chunk_id, chunk.content AS content
                """,
                chunk_ids=chunk_ids
            )
            chunks = []
            async for record in result:
                chunks.append({"chunk_id": record["chunk_id"], "content": record["content"]})
            return chunks
    
    async def get_next_uid_async(self, doc_id):
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (entity:Entity {doc_id: $doc_id})
                RETURN MAX(entity.uid) AS max_uid
                """,
                doc_id=doc_id
            )
            record = await result.single()
            max_uid = record["max_uid"] if record and record["max_uid"] is not None else 0
            return max_uid + 1
    
    async def batch_create_entity_nodes_async(self, entity_data, file_name, doc_id):
        """
        Batch create entity nodes using pre-merged and embedded data.
        This function assumes that merging has been done in Redis and simply creates new nodes.
        """
        async with self.driver.session() as session:
            await session.write_transaction(
                self._batch_create_entity_nodes_tx_async, entity_data, file_name, doc_id
            )
    
    @staticmethod
    async def _batch_create_entity_nodes_tx_async(tx, entity_data, file_name, doc_id):
        # 🚀 Prepare a list of entities for a single UNWIND query.
        entities_list = [
            {
                "name": name,
                "file_name": file_name,
                "doc_id": doc_id,
                "type": data["type"],
                "uid": index + 1,  # Assign UID incrementally
                "descriptions": data["descriptions"],
                "embedding": data["embedding"],
                "step_to_assist_user": data.get("step_to_assist_user")
            }
            for index, (name, data) in enumerate(entity_data.items())
        ]
        # Create all entities in a single query.
        await tx.run(
            """
            UNWIND $entities AS entity_data
            CREATE (entity:Entity {
                name: entity_data.name, 
                file_name: entity_data.file_name, 
                doc_id: entity_data.doc_id, 
                type: entity_data.type, 
                uid: entity_data.uid, 
                descriptions: entity_data.descriptions,
                embedding: entity_data.embedding,
                step_to_assist_user: entity_data.step_to_assist_user
            })
            """,
            entities=entities_list
        )
        logging.info(f"Batch created {len(entities_list)} entity nodes for document {doc_id}.")
        
        # Batch link chunks to entities using a single query.
        links = [
            {"chunk_id": chunk_id, "entity_name": entity_name, "doc_id": doc_id}
            for entity_name, data in entity_data.items()
            for chunk_id in data["chunks"]
        ]
        if links:
            await tx.run(
                """
                UNWIND $links AS link
                MATCH (chunk:Chunk {chunk_id: link.chunk_id})
                MATCH (entity:Entity {name: link.entity_name, doc_id: link.doc_id})
                MERGE (chunk)-[:MENTIONS]->(entity)
                """,
                links=links
            )
            logging.info(f"Batch linked chunks to entities for document {doc_id}.")
    
    async def get_entities_without_embedding_async(self, doc_id, limit=100):
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (entity:Entity {doc_id: $doc_id})
                WHERE entity.embedding IS NULL
                RETURN entity.uid AS uid, entity.descriptions AS descriptions
                LIMIT $limit
                """,
                doc_id=doc_id,
                limit=limit
            )
            entities = []
            async for record in result:
                entities.append({"uid": record["uid"], "descriptions": record["descriptions"]})
            return entities
    
    async def update_entity_node_with_embedding_async(self, uid, doc_id, description_embedding):
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (entity:Entity {uid: $uid, doc_id: $doc_id})
                SET entity.embedding = $description_embedding
                """,
                uid=uid,
                doc_id=doc_id,
                description_embedding=description_embedding
            )
            logging.info(f"Updated entity with UID '{uid}' and doc_id '{doc_id}' with embeddings.")
    
    async def attach_high_level_description_async(self, doc_id, high_des):
        async with self.driver.session() as session:
            await session.run(
                """
                MATCH (doc:Document {doc_id: $doc_id})
                SET doc.high_des = $high_des
                """,
                doc_id=doc_id,
                high_des=high_des
            )
            logging.info(f"High-level description attached to document {doc_id}.")
    
    async def get_all_entities_by_doc_id_async(self, doc_id):
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (entity:Entity {doc_id: $doc_id})
                RETURN entity.descriptions AS descriptions
                """,
                doc_id=doc_id
            )
            descriptions = []
            async for record in result:
                if record["descriptions"]:
                    descriptions.append(record["descriptions"])
            return descriptions
    
    async def create_vector_index_async(self, label, property_name, index_name, dimensions=768):
        async with self.driver.session() as session:
            result = await session.run("SHOW INDEXES YIELD name RETURN name")
            index_names = []
            async for record in result:
                index_names.append(record["name"].strip('"'))
    
            if index_name not in index_names:
                await session.run(
                    f"""
                    CREATE VECTOR INDEX {index_name} FOR (n:{label}) ON (n.{property_name})
                    OPTIONS {{
                        indexConfig: {{
                            `vector.dimensions`: $dimensions,
                            `vector.similarity_function`: 'cosine'
                        }}
                    }}
                    """,
                    dimensions=dimensions
                )
                logging.info(f"Vector index '{index_name}' created successfully.")
            else:
                logging.info(f"Vector index '{index_name}' already exists, skipping creation.")