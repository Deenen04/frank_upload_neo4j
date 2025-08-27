import logging
import os
import sys
from typing import List, Dict, Any, Optional
from collections import deque
import asyncio
import httpx
import json
from dotenv import load_dotenv
load_dotenv()

# Import configuration constants
from config import OPENAI_API_URL, MAX_RETRIES, RETRY_BACKOFF_FACTOR, DEFAULT_TIMEOUT

# Import logging configuration from utils.py
from utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)


class APIKeyManager:
    def __init__(self, api_keys: List[str]):
        self.api_keys = deque(api_keys)
        self.usage_count = {key: 0 for key in api_keys}  # Track usage for each key
        self.lock = asyncio.Lock()

    @classmethod
    def from_env(cls, env_var: str) -> 'APIKeyManager':
        OPENAI_API_KEYS = os.getenv(env_var)
        if not OPENAI_API_KEYS:
            logger.error(f"{env_var} environment variable is not set.")
            raise EnvironmentError(f"Please set the {env_var} environment variable.")

        api_keys = [key.strip().lstrip('=') for key in OPENAI_API_KEYS.split(",") if key.strip().startswith('sk-')]

        if not api_keys:
            logger.error("No valid API keys found in OPENAI_API_KEYS environment variable.")
            raise ValueError("OPENAI_API_KEYS must contain at least one valid API key.")

        return cls(api_keys=api_keys)

    async def get_next_api_key(self) -> str:
        async with self.lock:
            for _ in range(len(self.api_keys)):  # Loop through keys
                key = self.api_keys[0]
                if self.usage_count[key] < 80:  # Check if key usage is below 60
                    self.usage_count[key] += 1
                    logger.debug(f"Using API key: {key[:5]}*** (Usage: {self.usage_count[key]}/80)")
                    return key
                else:
                    # Rotate key and reset usage if needed
                    self.api_keys.rotate(-1)
                    if self.usage_count[self.api_keys[0]] == 80:
                        self.usage_count[self.api_keys[0]] = 0



class APIClient:
    def __init__(self, api_key_manager: APIKeyManager, timeout: int = DEFAULT_TIMEOUT):
        self.api_key_manager = api_key_manager
        self.client = httpx.AsyncClient(timeout=timeout)

    async def make_openai_request(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: int,
        temperature: float,
        top_p: float
    ) -> Optional[Dict[str, Any]]:
        retries = 0
        while retries < MAX_RETRIES:
            api_key = await self.api_key_manager.get_next_api_key()
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            payload = {
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": top_p
            }
            try:
                logger.debug(f"Sending request with API key: {api_key[:5]}***")
                response = await self.client.post(OPENAI_API_URL, headers=headers, json=payload)
                response.raise_for_status()
                logger.debug("API request successful.")
                return response.json()
            except httpx.HTTPStatusError as http_err:
                status = http_err.response.status_code
                logger.error(f"HTTP error {status}: {http_err.response.text}")
                if status in {429, 500, 502, 503, 504}:
                    # Retry on rate limit or server errors
                    retries += 1
                    backoff = RETRY_BACKOFF_FACTOR ** retries
                    logger.info(f"Retrying in {backoff} seconds...")
                    await asyncio.sleep(backoff)
                else:
                    # Do not retry on client errors
                    break
            except httpx.RequestError as req_err:
                logger.error(f"Request error: {req_err}")
                retries += 1
                backoff = RETRY_BACKOFF_FACTOR ** retries
                logger.info(f"Retrying in {backoff} seconds...")
                await asyncio.sleep(backoff)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break
        logger.error("Max retries exceeded. Request failed.")
        return None

    async def close(self):
        await self.client.aclose()
        
        
    async def make_openai_request_reasoning_model(
        self,
        model: str,
        messages: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        retries = 0
        while retries < MAX_RETRIES:
            api_key = await self.api_key_manager.get_next_api_key()
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            payload = {
                "model": model,
                "messages": messages            
                }
            try:
                logger.debug(f"Sending request with API key: {api_key[:5]}***")
                response = await self.client.post(OPENAI_API_URL, headers=headers, json=payload)
                response.raise_for_status()
                logger.debug("API request successful.")
                return response.json()
            except httpx.HTTPStatusError as http_err:
                status = http_err.response.status_code
                logger.error(f"HTTP error {status}: {http_err.response.text}")
                if status in {429, 500, 502, 503, 504}:
                    # Retry on rate limit or server errors
                    retries += 1
                    backoff = RETRY_BACKOFF_FACTOR ** retries
                    logger.info(f"Retrying in {backoff} seconds...")
                    await asyncio.sleep(backoff)
                else:
                    # Do not retry on client errors
                    break
            except httpx.RequestError as req_err:
                logger.error(f"Request error: {req_err}")
                retries += 1
                backoff = RETRY_BACKOFF_FACTOR ** retries
                logger.info(f"Retrying in {backoff} seconds...")
                await asyncio.sleep(backoff)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break
        logger.error("Max retries exceeded. Request failed.")
        return None

    async def close(self):
        await self.client.aclose()


async def make_openai_request(
    api_key_manager: APIKeyManager,
    model: str,
    messages: List[Dict[str, Any]],
    max_tokens,
    temperature: float,
    top_p: float
) -> Optional[Dict[str, Any]]:
    api_client = APIClient(api_key_manager=api_key_manager)
    try:
        response = await api_client.make_openai_request(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p
        )
        
        return response.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
    finally:
        await api_client.close()
        
        
async def make_openai_request_reasoning_model(
    api_key_manager: APIKeyManager,
    model: str,
    messages: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    api_client = APIClient(api_key_manager=api_key_manager)
    try:
        response = await api_client.make_openai_request_reasoning_model(
            model=model,
            messages=messages
        )
        return response.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
    finally:
        await api_client.close()


# Example usage: Test function
async def main():
    try:
        # Initialize APIKeyManager from environment variable
        api_key_manager = APIKeyManager.from_env("AI_HIGH_LEVEL_DESCRIPTION")

        # Example API request parameters
        model = "gpt-4o-mini"
        messages = [
            {
                "role": "user",
                "content": "Hello, how can I assist you today?"
            }
        ]
        max_tokens = 150
        temperature = 0.7
        top_p = 0.9

        # Make an API request
        response = await make_openai_request(
            api_key_manager=api_key_manager,
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p
        )

        if response:
            content = response
            print("API Response:")
            print(content)
        else:
            print("Failed to get a response from the API.")
    except Exception as e:
        logger.error(f"An error occurred in main: {e}")


def handle_api_response(response):
    try:
        data = json.loads(response)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing AI response as JSON: {e}")


if __name__ == "__main__":
    asyncio.run(main())