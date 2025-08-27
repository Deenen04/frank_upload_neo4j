import asyncio
import logging
from main import process_document, status_worker, status_queue
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()

async def test_document_processing():
    """
    Test function to process a document and push it to Neo4j
    """
    try:
        # === Replace these values with your actual configuration ===
        
        # PDF file path
        pdf_path = "FAQ.pdf"  # Replace with your PDF path

        # Neo4j connection details
        neo4j_uri = "bolt://localhost:7687"  # Replace with your Neo4j URI
        neo4j_user = "neo4j"  # Replace with your Neo4j username
        neo4j_password = "YourPassword123"  # Replace with your Neo4j password

        # Document metadata
        file_name = "Babar SWISS BOT.pdf"  # Replace with your file name
        
        # Optional: If you want to specify a document ID
        doc_id = "Swiss_BOT"# Replace with your doc_id if needed
        UID = "1"  # Replace with your UID if needed

        # Start the status worker
        worker_task = asyncio.create_task(status_worker())

        try:
            # Process the document
            result = await process_document(
                pdf_path=pdf_path,
                neo4j_uri=neo4j_uri,
                neo4j_user=neo4j_user,
                neo4j_password=neo4j_password,
                file_name=file_name,
                doc_id=doc_id,
                UID=UID
            )
            
            print("Processing Result:", result)
            
        finally:
            # Signal the worker to shut down
            await status_queue.put(None)
            await worker_task

    except Exception as e:
        logging.error(f"Error during document processing: {e}")
        raise

if __name__ == "__main__":
    # Run the test
    asyncio.run(test_document_processing()) 