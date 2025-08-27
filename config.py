import os
from dotenv import load_dotenv


# Load environment variables from .env if available
load_dotenv()

#API KEYS FOR S3
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


# Configuration constants
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_EMBEDDING_URL = "https://api.openai.com/v1/embeddings"
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2  # Exponential backoff factor
DEFAULT_TIMEOUT = 700  # seconds




# Fetch Neo4j credentials from environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

redis_url = os.getenv("REDIS_URL")
if not redis_url:
    raise ValueError("REDIS_URL environment variable not set.")

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("REGION_NAME")