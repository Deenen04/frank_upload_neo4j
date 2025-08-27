# embed_text.py

import asyncio
import logging
from typing import List
from sentence_transformers import SentenceTransformer
import torch

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Load model once at module level
model = SentenceTransformer('intfloat/e5-base-v2')

async def get_embeddings(texts):
    """
    Async function to get embeddings for one or multiple texts.
    
    Args:
        texts (str or list of str): Input text(s)
        
    Returns:
        list: Embedding(s) as a list of floats
    """
    if isinstance(texts, str):
        texts = [texts]

    # E5-style input format
    inputs = [f"query: {t}" for t in texts]

    loop = asyncio.get_event_loop()
    embeddings = await loop.run_in_executor(None, lambda: model.encode(inputs, convert_to_numpy=True))

    if len(embeddings) == 1:
        return embeddings[0].tolist()
    return [e.tolist() for e in embeddings]

async def get_embeddings_async(texts: List[str]) -> List[List[float]]:
    """
    Retrieve embeddings for multiple texts asynchronously using sentence transformers.
    This function maintains the same interface as the original OpenAI version.
    
    Args:
        texts (List[str]): The list of texts to embed.
    
    Returns:
        List[List[float]]: A list of embeddings.
    """
    if not texts:
        return []
    
    logging.info(f"Starting embedding for {len(texts)} texts using sentence transformer...")
    
    try:
        # E5-style input format
        inputs = [f"query: {t}" for t in texts]
        
        loop = asyncio.get_event_loop()
        embeddings = await loop.run_in_executor(None, lambda: model.encode(inputs, convert_to_numpy=True))
        
        # Convert to list of lists
        result = [e.tolist() for e in embeddings]
        
        logging.info(f"Embedding successful for {len(texts)} texts using sentence transformer.")
        return result
        
    except Exception as e:
        logging.error(f"Exception during embedding with sentence transformer: {e}")
        return [None] * len(texts)

# Keep these functions for backward compatibility if they're used elsewhere
async def fetch_embedding(session, text: str) -> List:
    """Fetch a single embedding using sentence transformers."""
    try:
        result = await get_embeddings([text])
        return [result if isinstance(result, list) else result]
    except Exception as e:
        logging.error(f"Exception during single embedding: {e}")
        return [None]