import asyncio
import logging
import json
from extract_text import selectively_extract_text_async
from chunk_text import chunk_text_by_rules_async
from embed_text import get_embeddings_async  # Updated to use batch embedding
from create_nodes import AsyncNeo4jHandler
from analyze_relationships import analyze_chunks_with_llm_async
from parse_llm_response import parse_llm_response
from ai_high_level_description import process_descriptions_and_combine_async
from collections import defaultdict
import re
import os
from dotenv import load_dotenv
import redis
from config import redis_url # Assuming config.py defines redis_url
import sys

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Standard Redis client (if needed) - Note: redis_url is defined again later from os.getenv
# client = redis.Redis.from_url(redis_url, decode_responses=True) # This might be using the imported redis_url

# NEW: Using asyncio Redis client
import redis.asyncio as aioredis

# Load environment variables from .env if available
load_dotenv()

# Global status queue for tracking progress
status_queue = asyncio.Queue()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Redis connection
# Ensure redis_url is taken from environment after load_dotenv()
redis_env_url = os.getenv("REDIS_URL")
if not redis_env_url:
    raise ValueError("REDIS_URL environment variable not set.")

# Use the environment variable for the async client
redis_client = aioredis.from_url(redis_env_url, decode_responses=True)


async def status_worker():
    """
    Background task that consumes status updates from the status_queue
    and updates the corresponding Redis JSON object.
    """
    current_doc_id = None
    buffer = []

    logging.info("Status worker started.")

    while True:
        status = await status_queue.get()
        if status is None:
            logging.info("Status worker received shutdown signal.")
            break

        logging.info(f"Status update: {status}")

        if 'doc_id' in status:
            current_doc_id = status['doc_id']
            await redis_client.set(f"document_status:{current_doc_id}", json.dumps({
                "messages": [],
                "percentage": 0
            }))
            logging.info(f"Initialized Redis key for doc_id: {current_doc_id}")
            for buffered_status in buffer:
                await update_redis(current_doc_id, buffered_status)
            buffer.clear()

        if current_doc_id:
            await update_redis(current_doc_id, status)
            if status.get("percentage") == 100 or "Error during processing" in status.get("message", ""):
                logging.info(f"Final status detected for doc_id: {current_doc_id}. Scheduling deletion.")
                # It's generally better to let the main processing function handle cleanup after completion or error.
                # However, if this is intended as a final failsafe:
                # await asyncio.sleep(10)
                # await redis_client.delete(f"document_status:{current_doc_id}") # This specific key deletion might be okay.
                # logging.info(f"Deleted Redis key for doc_id: {current_doc_id}") # Consider if cleanup_redis_keys should handle this.
        else:
            buffer.append(status)

        status_queue.task_done()


async def update_redis(doc_id, status):
    """
    Updates the Redis JSON object for the given doc_id with the new status.
    """
    key = f"document_status:{doc_id}"
    try:
        existing_data_json = await redis_client.get(key)
        if existing_data_json:
            data = json.loads(existing_data_json)
        else:
            data = {"messages": [], "percentage": 0}

        if 'message' in status:
            data['messages'].append(status['message'])
        if 'percentage' in status:
            data['percentage'] = status['percentage']

        await redis_client.set(key, json.dumps(data))
        logging.info(f"Updated Redis key {key}: {data}")
    except Exception as e:
        logging.error(f"Failed to update Redis for doc_id {doc_id}: {e}")


async def cleanup_redis_keys(doc_id):
    """
    Deletes all Redis keys created for the given doc_id.
    This includes:
      - The status key: "document_status:{doc_id}"
      - All keys starting with "{doc_id}:"
    """
    keys_to_delete = []
    status_key = f"document_status:{doc_id}"
    keys_to_delete.append(status_key)
    
    # Using scan_iter for potentially large number of keys
    try:
        async for key in redis_client.scan_iter(f"{doc_id}:*"):
            keys_to_delete.append(key)
    except Exception as e:
        logging.error(f"Error fetching keys for cleanup for doc_id {doc_id} using scan_iter: {e}")
        # Fallback or alternative if scan_iter has issues with the client version or setup
        try:
            other_keys = await redis_client.keys(f"{doc_id}:*")
            keys_to_delete.extend(other_keys)
        except Exception as e_keys:
            logging.error(f"Error fetching keys for cleanup for doc_id {doc_id} using keys: {e_keys}")

    if keys_to_delete:
        # Remove duplicates if any (e.g., if status_key somehow matched pattern)
        unique_keys_to_delete = list(set(keys_to_delete))
        try:
            if unique_keys_to_delete: # Ensure list is not empty
                await redis_client.delete(*unique_keys_to_delete)
                logging.info(f"Cleaned up Redis keys for doc_id {doc_id}: {unique_keys_to_delete}")
        except Exception as e:
            logging.error(f"Error during Redis cleanup for doc_id {doc_id}: {e}")


async def process_document(pdf_path, neo4j_uri, neo4j_user, neo4j_password, file_name, doc_id=None, UID=None): # UID is unused
    """
    Asynchronously processes a document by extracting text, chunking it, embedding entities,
    creating nodes in Neo4j, and generating high-level descriptions.
    
    Returns:
        dict: Contains the 'doc_id' upon successful processing.
    """
    global status_queue # status_queue is already global, no need for 'global' keyword here

    # Ensure doc_id is determined early for status reporting
    neo4j_handler_instance = None # Initialize to ensure it's defined for finally block
    # If doc_id is not provided, it will be generated after Neo4j handler is initialized.
    # This means initial status messages might not have doc_id yet if it's generated.
    # Let's adjust status reporting for this.

    # Temporary doc_id for initial messages if not provided
    temp_process_id = doc_id if doc_id else f"pending_{asyncio.get_event_loop().time()}"
    await status_queue.put({"doc_id": temp_process_id, "message": "Initializing document processing.", "percentage": 0})
   
    start_time = asyncio.get_event_loop().time()
   
    try:
        # Initialize Neo4j handler and create vector index
        neo4j_handler_instance = AsyncNeo4jHandler(neo4j_uri, neo4j_user, neo4j_password)
        await neo4j_handler_instance.create_vector_index_async("Entity", "embedding", "vector_entities_index")
       
        if not doc_id:
            doc_id = await generate_new_doc_id_async(neo4j_handler_instance)
            # If doc_id was generated, update the status system to use the new doc_id
            if temp_process_id != doc_id:
                # This requires status_worker to handle doc_id changes or re-initialization.
                # For simplicity, we ensure doc_id is available for all subsequent status updates.
                # The initial message for temp_process_id will remain. A new init for actual doc_id might be cleaner.
                await status_queue.put({"doc_id": doc_id, "message": f"Generated document ID: {doc_id}", "percentage": 1}) # Small percentage update

        logging.info(f"Starting processing for doc_id: {doc_id}")
        await status_queue.put({"doc_id": doc_id, "message": "Connecting to Server...", "percentage": 5})

        # Create document node in Neo4j
        await status_queue.put({"doc_id": doc_id, "message": "Creating document node...", "percentage": 15})
        await neo4j_handler_instance.create_document_node_async(doc_id, file_name, "2024-10-19") # Date seems hardcoded

        # Extract text from PDF asynchronously
        await status_queue.put({"doc_id": doc_id, "message": "Extracting text from PDF...", "percentage": 20})
        text_by_page = await selectively_extract_text_async(pdf_path, None, None)

        # Chunk text asynchronously
        await status_queue.put({"doc_id": doc_id, "message": "Chunking text...", "percentage": 30})
        chunks, metadata = await chunk_text_by_rules_async(text_by_page)

        # Create chunk nodes in Neo4j
        await status_queue.put({"doc_id": doc_id, "message": "Creating chunk nodes...", "percentage": 50})
        await neo4j_handler_instance.batch_create_chunk_nodes_async(doc_id, chunks, file_name, metadata)

        # === Part 2: Save chunks to Redis in batches of 100 for AI processing ===
        batch_size_redis = 100
        for batch_index, start_index in enumerate(range(0, len(chunks), batch_size_redis)):
            batch_chunks = chunks[start_index:start_index+batch_size_redis]
            batch_metadata = metadata[start_index:start_index+batch_size_redis]
            redis_batch_data = []
            for i, (chunk_text, meta) in enumerate(zip(batch_chunks, batch_metadata)):
                chunk_id_val = f"{doc_id}_chunk_{start_index + i + 1}" # Renamed to avoid conflict
                redis_batch_data.append({
                    "chunk_id": chunk_id_val,
                    "text": chunk_text,
                    "chunk_index": meta['chunk_index'],
                    "file_name": file_name
                })
            redis_key = f"{doc_id}:chunks:batch_{batch_index+1}"
            await redis_client.set(redis_key, json.dumps(redis_batch_data))
            logging.info(f"Stored batch {batch_index+1} of chunks in Redis with key {redis_key}.")

        # === Part 3: Process each Redis batch concurrently with AI analysis (up to 100 at once) ===
        entity_data = defaultdict(lambda: {"type": None, "descriptions": set(), "chunks": set()})
        semaphore = asyncio.Semaphore(100) # Max concurrent AI calls

        async def process_chunk(chunk_info): # Renamed 'chunk' to 'chunk_info' to avoid conflict with outer scope 'chunks' list
            async with semaphore:
                try:
                    # `analyze_chunks_with_llm_async` now returns the raw text content directly (single value).
                    ai_result_content = await analyze_chunks_with_llm_async(chunk_info["text"])
                except Exception as e:
                    logging.error(f"AI analysis failed for chunk {chunk_info['chunk_id']}: {e}")
                    return # Skip this chunk on AI call failure

                if not ai_result_content: # Check if the result is None or an empty string
                    logging.warning(f"No analysis result for chunk {chunk_info['chunk_id']} (empty AI response)")
                    return # Skip if AI returns no analysable content
                
                # `ai_result_content` is now the string that needs to be parsed.
                # The try-except block that might have previously caught unpacking errors
                # is now focused on errors from `parse_llm_response`.
                try:
                    # `parse_llm_response` takes the raw content string.
                    batch_entities = parse_llm_response(ai_result_content)
                except Exception as e:
                    # This error message is consistent with an "Invalid analysis format" if parsing fails.
                    # Adding a snippet of the content for easier debugging.
                    content_snippet = str(ai_result_content)[:200] + "..." if len(str(ai_result_content)) > 200 else str(ai_result_content)
                    logging.error(f"Invalid analysis format for chunk {chunk_info['chunk_id']} (parsing LLM response failed): {e}. Content snippet: '{content_snippet}'")
                    return # Skip this chunk if parsing fails
                
                # Process entities. The original code's use of .get(1, []) is preserved.
                # This assumes parse_llm_response returns a dictionary where key 1 contains a list of entity tuples.
                # Example: batch_entities = {1: [("EntityName1", "Type1", "Desc1"), ("EntityName2", "Type2", "Desc2")]}
                
                if not isinstance(batch_entities, dict):
                    logging.error(f"parse_llm_response for chunk {chunk_info['chunk_id']} was expected to return a dict, but got {type(batch_entities)}. Value: '{str(batch_entities)[:200]}'")
                    return # Avoid AttributeError if .get() is called on non-dict

                entities_from_dict_key = batch_entities.get(1, []) # Get list from dict key 1, default to empty list

                if not isinstance(entities_from_dict_key, list):
                    logging.error(f"Value from batch_entities.get(1, []) for chunk {chunk_info['chunk_id']} was expected to be a list, but got {type(entities_from_dict_key)}. Value: '{str(entities_from_dict_key)[:200]}'")
                    return

                for entity_tuple in entities_from_dict_key:
                    if not (isinstance(entity_tuple, (tuple, list)) and len(entity_tuple) == 4):
                        logging.warning(f"Skipping malformed entity entry in chunk {chunk_info['chunk_id']}: {entity_tuple}. Expected 4 values (name, type, description, step_to_assist_user).")
                        continue

                    entity_name, entity_type, description, step_to_assist_user = entity_tuple

                    # Update the shared entity_data; set operations are generally safe with CPython's GIL
                    # for these simple types, but be mindful if entity_data becomes more complex.
                    entity_data[entity_name]["type"] = entity_type # Last one wins for type if multiple definitions
                    entity_data[entity_name]["descriptions"].add(description)
                    entity_data[entity_name]["chunks"].add(chunk_info["chunk_id"])
                    # Optional field retained
                    entity_data[entity_name]["step_to_assist_user"] = step_to_assist_user

        num_batches = (len(chunks) + batch_size_redis - 1) // batch_size_redis
        for batch_idx in range(num_batches): # Renamed batch_index to batch_idx
            redis_key_batch = f"{doc_id}:chunks:batch_{batch_idx+1}" # Renamed redis_key
            batch_json = await redis_client.get(redis_key_batch)
            if not batch_json:
                logging.warning(f"Redis batch {redis_key_batch} not found. Skipping.")
                continue
            
            try:
                current_batch_data = json.loads(batch_json)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON from Redis key {redis_key_batch}: {e}")
                continue

            await status_queue.put({"doc_id": doc_id, "message": f"Processing Redis batch {batch_idx+1} with AI analysis...", "percentage": 60 + int((batch_idx / num_batches) * 20)}) # Dynamic percentage
            
            tasks = [process_chunk(chunk_detail) for chunk_detail in current_batch_data] # Renamed chunk to chunk_detail
            # Using return_exceptions=True to gather all results, even if some tasks fail
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res_idx, res in enumerate(results):
                if isinstance(res, Exception):
                    failed_chunk_id = current_batch_data[res_idx].get('chunk_id', 'unknown_chunk_in_batch')
                    logging.error(f"Error processing chunk {failed_chunk_id} in batch {batch_idx+1}: {res}")


            # Save merged entity data in Redis (update after each batch)
            redis_entity_key = f"{doc_id}:entities"
            merged_entities_for_redis = {
                k: {
                    "type": v["type"],
                    "descriptions": list(v["descriptions"]),
                    "chunks": list(v["chunks"]),
                    "step_to_assist_user": v.get("step_to_assist_user")
                }
                for k, v in entity_data.items()
            }
            await redis_client.set(redis_entity_key, json.dumps(merged_entities_for_redis))
            logging.info(f"Updated merged entity data in Redis key {redis_entity_key} after batch {batch_idx+1}")

        # === Part 4: Embed the merged entity descriptions and send final entity data to Neo4j ===
        await status_queue.put({"doc_id": doc_id, "message": "Embedding entities...", "percentage": 85})
        redis_entity_key_final = f"{doc_id}:entities" # Renamed
        merged_entities_json = await redis_client.get(redis_entity_key_final)
        
        final_merged_entities = {} # Renamed
        if merged_entities_json:
            try:
                final_merged_entities = json.loads(merged_entities_json)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode merged entities JSON from Redis key {redis_entity_key_final}: {e}")
                final_merged_entities = {} # Proceed with empty if unparsable
        
        if final_merged_entities:
            entity_names = list(final_merged_entities.keys())
            descriptions_to_embed = [" ".join(final_merged_entities[name]["descriptions"]) for name in entity_names]

            if descriptions_to_embed: # Ensure there are descriptions to embed
                batch_embeddings = await get_embeddings_async(descriptions_to_embed)

                entity_data_for_neo4j = { # Renamed
                    name: {
                        "type": final_merged_entities[name]["type"],
                        "descriptions": final_merged_entities[name]["descriptions"],
                        "chunks": final_merged_entities[name]["chunks"],
                        "embedding": embedding,
                        "step_to_assist_user": final_merged_entities[name].get("step_to_assist_user")
                    }
                    for name, embedding in zip(entity_names, batch_embeddings) if embedding is not None # Guard against None embeddings
                }
                if entity_data_for_neo4j:
                    await neo4j_handler_instance.batch_create_entity_nodes_async(entity_data_for_neo4j, file_name, doc_id)
            else:
                logging.info(f"No descriptions found to embed for doc_id {doc_id}.")
        else:
            logging.info(f"No merged entities found in Redis for doc_id {doc_id} to embed.")


        # === Part 5: Generate high-level description from Redis and attach to the document node ===
        logging.info(f"Generating high-level description for document {doc_id} from Redis.")
        all_descriptions_for_summary = [] # Renamed
        
        # Retrieve merged entities again from Redis (or use final_merged_entities if still valid)
        # Re-getting might be safer if there's a long delay or complex logic.
        merged_entities_json_for_summary = await redis_client.get(redis_entity_key_final) # Use the same key
        if merged_entities_json_for_summary:
            try:
                entities_for_summary = json.loads(merged_entities_json_for_summary) # Renamed
                for _, entity_info in entities_for_summary.items():
                    all_descriptions_for_summary.extend(entity_info["descriptions"])
            except json.JSONDecodeError as e:
                 logging.error(f"Failed to decode merged entities for summary from Redis key {redis_entity_key_final}: {e}")
        else:
            logging.warning(f"No merged entities found in Redis for doc_id {doc_id} to generate summary.")
        
        if all_descriptions_for_summary:
            high_level_summary = await process_descriptions_and_combine_async(all_descriptions_for_summary)
            if high_level_summary:
                # Attach high-level description to the document node
                await status_queue.put({"doc_id": doc_id, "message": "Attaching high-level description...", "percentage": 95})
                
                # Convert high_level_summary to JSON string before storing in Neo4j
                high_level_summary_json = json.dumps(high_level_summary)
                
                await neo4j_handler_instance.attach_high_level_description_async(doc_id, high_level_summary_json)
                logging.info(f"High-level description attached to document {doc_id}.")
            else:
                logging.warning(f"Generated high-level summary was empty for document {doc_id}.")

        else:
            logging.warning(f"No descriptions available for generating high-level summary for document {doc_id}.")

        await status_queue.put({"doc_id": doc_id, "message": "Document processed successfully.", "percentage": 100})
        logging.info(f"Document processing completed successfully for doc_id: {doc_id}")
        
        return {"doc_id": doc_id}

    except Exception as e:
        logging.error(f"Error in main process_document for doc_id {doc_id if doc_id else temp_process_id}: {e}", exc_info=True)
        await status_queue.put({"doc_id": doc_id if doc_id else temp_process_id, "message": f"Error during processing: {e}", "percentage": 0}) # Reset percentage on error
        # Re-raise the exception so the caller knows something went wrong.
        raise
    finally:
        end_time = asyncio.get_event_loop().time()
        logging.info(f"Document processing for doc_id {doc_id if doc_id else temp_process_id} took {end_time - start_time:.2f} seconds.")
        
        if neo4j_handler_instance:
            try:
                await neo4j_handler_instance.close()
            except Exception as close_error:
                logging.error(f"Error closing neo4j_handler for doc_id {doc_id if doc_id else temp_process_id}: {close_error}")
        
        # Cleanup all Redis keys associated with this doc_id
        # Ensure doc_id is the final one if it was generated.
        actual_doc_id_for_cleanup = doc_id if doc_id else None # temp_process_id keys might not follow {doc_id}:* pattern
        if actual_doc_id_for_cleanup:
            try:
                await cleanup_redis_keys(actual_doc_id_for_cleanup)
            except Exception as cleanup_e:
                logging.error(f"Failed to cleanup Redis keys for doc_id {actual_doc_id_for_cleanup}: {cleanup_e}")
        elif temp_process_id and not doc_id: # If processing failed before doc_id generation
             # Cleanup status key for temp_process_id if it exists
            try:
                await redis_client.delete(f"document_status:{temp_process_id}")
                logging.info(f"Cleaned up status key for temp_process_id: {temp_process_id}")
            except Exception as e:
                logging.error(f"Failed to cleanup status key for temp_process_id {temp_process_id}: {e}")


async def generate_new_doc_id_async(neo4j_handler):
    """
    Generates a new unique document ID by incrementing the highest existing ID.
    """
    existing_doc_ids = await neo4j_handler.get_existing_doc_ids_async()
    if not existing_doc_ids:
        return "document_1"
    
    doc_numbers = []
    for doc_id_str in existing_doc_ids: # Renamed doc_id to doc_id_str
        match = re.search(r'document_(\d+)', doc_id_str) # More specific regex
        if match:
            try:
                doc_numbers.append(int(match.group(1)))
            except ValueError:
                logging.warning(f"Could not parse document number from ID: {doc_id_str}")
        
    max_doc_number = max(doc_numbers, default=0)
    return f"document_{max_doc_number + 1}"


# Expose status_worker and status_queue for api.py to manage
__all__ = ["status_worker", "status_queue", "process_document", "generate_new_doc_id_async"]


if __name__ == "__main__":
    # import sys # Already imported

    async def main_cli(): # Renamed main to main_cli to avoid conflict
        if len(sys.argv) < 6: # Adjusted for 5 arguments + script name
            print("Usage: python main.py <pdf_path> <neo4j_uri> <neo4j_user> <neo4j_password> <file_name>")
            sys.exit(1) # Exit if not enough arguments

        # sys.argv[0] is the script name
        pdf_path_arg = sys.argv[1]
        neo4j_uri_arg = sys.argv[2]
        neo4j_user_arg = sys.argv[3]
        neo4j_password_arg = sys.argv[4]
        file_name_arg = sys.argv[5]

        # Start the status worker
        worker_task = asyncio.create_task(status_worker())

        try:
            result = await process_document(
                pdf_path=pdf_path_arg,
                neo4j_uri=neo4j_uri_arg,
                neo4j_user=neo4j_user_arg,
                neo4j_password=neo4j_password_arg,
                file_name=file_name_arg
            )
            print(f"Processing Result: {result}")
        except Exception as e:
            print(f"An error occurred during document processing: {e}")
        finally:
            # Signal the worker to shut down
            await status_queue.put(None)
            await worker_task # Wait for worker to finish
            # Close Redis connection
            if redis_client: # Check if redis_client was initialized
                await redis_client.close()
                await redis_client.connection_pool.disconnect() # Ensure pool is closed too
            logging.info("Main CLI execution finished.")

    asyncio.run(main_cli())