# import aioboto3
# import asyncio
# import logging
# from collections import defaultdict
# from config import aws_access_key_id, aws_secret_access_key, region_name

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# # Rate limiting constants
# MAX_REQUESTS_PER_SECOND = 25
# RATE_LIMIT_PERIOD = 1  # seconds

# class RateLimiter:
#     def __init__(self, max_requests: int, period: float):
#         self.max_requests = max_requests
#         self.period = period
#         self._semaphore = asyncio.Semaphore(max_requests)
#         self._lock = asyncio.Lock()
#         self._task = None  # Will be initialized in start()

#     async def _reset_semaphore(self):
#         while True:
#             await asyncio.sleep(self.period)
#             async with self._lock:
#                 released = self.max_requests - self._semaphore._value
#                 for _ in range(released):
#                     self._semaphore.release()

#     async def start(self):
#         if self._task is None:
#             self._task = asyncio.create_task(self._reset_semaphore())

#     async def acquire(self):
#         await self._semaphore.acquire()

#     async def stop(self):
#         if self._task:
#             self._task.cancel()
#             try:
#                 await self._task
#             except asyncio.CancelledError:
#                 pass
#             self._task = None

# async def selectively_extract_text_async(pdf_path: str, bucket_name: str, s3_file_prefix: str) -> dict:
#     """
#     Asynchronously uploads a PDF to S3, processes it with Textract, and retrieves extracted text by page.
    
#     Args:
#         pdf_path (str): Path to the PDF file.
#         bucket_name (str): Name of the S3 bucket.
#         s3_file_prefix (str): Prefix for the S3 object key.

#     Returns:
#         dict: A dictionary mapping page numbers to extracted text.
#     """
#     session = aioboto3.Session(
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#         region_name=region_name
#     )

#     rate_limiter = RateLimiter(MAX_REQUESTS_PER_SECOND, RATE_LIMIT_PERIOD)
#     await rate_limiter.start()

#     try:
#         async with session.client('s3') as s3_client, session.client('textract') as textract_client:
#             s3_file_name = f"{s3_file_prefix}.pdf"
            
#             # Upload the PDF to S3
#             await rate_limiter.acquire()
#             try:
#                 await s3_client.upload_file(pdf_path, bucket_name, s3_file_name)
#                 logging.info(f"Uploaded {pdf_path} to S3 bucket {bucket_name} as {s3_file_name}.")
#             except Exception as e:
#                 logging.error(f"Failed to upload {pdf_path} to S3: {e}")
#                 raise

#             # Start Textract document text detection (OCR only)
#             await rate_limiter.acquire()
#             try:
#                 response = await textract_client.start_document_text_detection(
#                     DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': s3_file_name}}
#                 )
#                 job_id = response['JobId']
#                 logging.info(f"Textract job started for {s3_file_name}. Job ID: {job_id}")
#             except Exception as e:
#                 logging.error(f"Failed to start Textract job for {s3_file_name}: {e}")
#                 raise

#             # Polling for job completion
#             logging.info(f"Waiting for Textract job {job_id} to complete...")
#             while True:
#                 await asyncio.sleep(5)  # Wait before polling again
#                 await rate_limiter.acquire()
#                 try:
#                     response = await textract_client.get_document_text_detection(JobId=job_id)
#                     status = response['JobStatus']
#                     logging.info(f"Textract job {job_id} status: {status}")
#                     if status == 'SUCCEEDED':
#                         logging.info(f"Textract job {job_id} completed successfully.")
#                         break
#                     elif status == 'FAILED':
#                         logging.error(f"Textract job {job_id} failed.")
#                         raise Exception(f"Textract job failed for {s3_file_name}")
#                     # Continue polling if IN_PROGRESS or other statuses
#                 except Exception as e:
#                     logging.error(f"Error while polling Textract job {job_id}: {e}")
#                     raise

#             # Retrieve text content by page
#             text_by_page = defaultdict(list)
#             next_token = None
#             while True:
#                 await rate_limiter.acquire()
#                 try:
#                     if next_token:
#                         response = await textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
#                     else:
#                         response = await textract_client.get_document_text_detection(JobId=job_id)
                    
#                     # Build a mapping of PAGE blocks to their page numbers
#                     page_blocks = {
#                         block['Id']: block.get('Page', 1)
#                         for block in response.get('Blocks', [])
#                         if block['BlockType'] == 'PAGE'
#                     }
                    
#                     for block in response.get('Blocks', []):
#                         if block['BlockType'] == 'LINE':
#                             # Use the page number provided if available
#                             page_number = block.get('Page', None)
#                             # If not provided, check for related PAGE blocks
#                             if page_number is None and 'Relationships' in block:
#                                 for relation in block['Relationships']:
#                                     if relation.get('Type') == 'CHILD':
#                                         for related_id in relation.get('Ids', []):
#                                             if related_id in page_blocks:
#                                                 page_number = page_blocks[related_id]
#                                                 break
#                                     if page_number is not None:
#                                         break
#                             # Default to page 1 if still not found
#                             if page_number is None:
#                                 page_number = 1
#                             text_by_page[page_number].append(block.get('Text', ''))

#                     next_token = response.get('NextToken')
#                     if not next_token:
#                         break
#                 except Exception as e:
#                     logging.error(f"Error while retrieving Textract results for job {job_id}: {e}")
#                     raise

#             # Combine lines for each page into single strings
#             text_by_page_combined = {page: "\n".join(lines) for page, lines in text_by_page.items()}

#             # Delete the file from S3 after processing
#             await delete_s3_file_async(s3_client, bucket_name, s3_file_name)

#             return text_by_page_combined

#     finally:
#         await rate_limiter.stop()

# async def delete_s3_file_async(s3_client, bucket_name: str, file_name: str):
#     """
#     Asynchronously deletes a file from S3.
    
#     Args:
#         s3_client: The aioboto3 S3 client.
#         bucket_name (str): Name of the S3 bucket.
#         file_name (str): Name of the file to delete.
#     """
#     try:
#         await s3_client.delete_object(Bucket=bucket_name, Key=file_name)
#         logging.info(f"Deleted {file_name} from S3 bucket {bucket_name}.")
#     except Exception as e:
#         logging.error(f"Failed to delete {file_name} from S3: {e}")


import logging
from collections import defaultdict
from pypdf import PdfReader

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

async def selectively_extract_text_async(pdf_path: str, bucket_name: str, s3_file_prefix: str) -> dict:
    """
    Extracts text from a PDF file using PyPDF and maps it by page.
    
    Args:
        pdf_path (str): Path to the PDF file.
        bucket_name (str): Name of the S3 bucket (not used in PyPDF version).
        s3_file_prefix (str): Prefix for the S3 object key (not used in PyPDF version).

    Returns:
        dict: A dictionary mapping page numbers to extracted text.
    """
    try:
        # Read the PDF file using PyPDF
        reader = PdfReader(pdf_path)
        all_text = "" # Initialize an empty string to hold all text

        for page_number, page in enumerate(reader.pages, start=1):
            text = page.extract_text()
            if text: # Append text only if it's not empty
                all_text += text + "\n" # Concatenate text and add a newline for separation
        
        logging.info(f"Successfully extracted text from {pdf_path} using PyPDF.")
        return all_text # Return the single string containing all text
    
    except Exception as e:
        logging.error(f"Failed to extract text from {pdf_path}: {e}")
        raise
