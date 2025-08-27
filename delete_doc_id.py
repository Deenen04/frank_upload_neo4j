from neo4j import GraphDatabase

# Neo4j credentials
neo4j_uri = "bolt://localhost:7687"  # Replace with your Neo4j URI
neo4j_user = "neo4j"  # Replace with your Neo4j username
neo4j_password = "YourPassword123"  # Replace with your Neo4j password

class VKGDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def delete_all_by_doc_id(self, doc_id):
        with self.driver.session() as session:
            session.write_transaction(self._delete_all_tx, doc_id)

    @staticmethod
    def _delete_all_tx(tx, doc_id):
        # Query to delete all Chunks, Entities, and the Document with the given doc_id
        query = """
        MATCH (doc:Document {doc_id: $doc_id})
        OPTIONAL MATCH (chunk:Chunk {doc_id: $doc_id})
        OPTIONAL MATCH (entity:Entity {doc_id: $doc_id})
        DETACH DELETE doc, chunk, entity
        """
        tx.run(query, doc_id=doc_id)

# Usage
vkg_db = VKGDatabase(neo4j_uri, neo4j_user, neo4j_password)

# Specify the doc_id to delete all related nodes
doc_id = "Swiss_BOT"

vkg_db.delete_all_by_doc_id(doc_id)
vkg_db.close()
