# ai_high_level_description.py
import asyncio
import logging
import json
from typing import List, Tuple, Dict
from apiChatCompletion import APIKeyManager, make_openai_request

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Create an asyncio.Queue for API keys
api_key_manager = APIKeyManager.from_env("AI_HIGH_LEVEL_DESCRIPTION")

async def process_descriptions(descriptions: List[str], batch_id: int) -> Dict:
    """
    Asynchronously process a batch of descriptions using the OpenAI model.
    """
    try:
        logging.info(f"Processing batch {batch_id} with {len(descriptions)} descriptions.")
        messages = [
            {"role": "system", "content": "You are a helpful assistant specializing in insurance documentation analysis."},
            {
                "role": "user",
                "content": f"""
You are tasked with creating a high-level overview of the following insurance-related descriptions. Your summary should provide a bird's-eye view of the content while maintaining insurance context.

Descriptions (Batch ID: {batch_id}):
{json.dumps(descriptions, indent=2)}

Your tasks:
1. Create a concise, high-level summary that captures the essence of the content
2. While maintaining the high-level perspective, ensure the summary reflects:
   - Overall themes in policy coverage and limitations
   - Common patterns in claims and administrative processes
   - General trends in plan options and customer inquiries
3. Focus on the big picture rather than specific details
4. Use insurance terminology appropriately but keep the summary accessible
                """
            }
        ]
        summary = await make_openai_request(
            api_key_manager=api_key_manager,
            model="gpt-4.1-nano",
            messages=messages,
            temperature=0.1,
            max_tokens=6000,
            top_p=0.1
        )
        logging.info(f"Batch {batch_id} processed successfully.")
        return {"batch_id": batch_id, "response": summary}
    except Exception as e:
        logging.error(f"Error processing batch {batch_id}: {e}")
        return {"batch_id": batch_id, "response": f"Error: {e}"}

async def combine_results(batch_responses: List[Dict]) -> Tuple[str, float]:
    """
    Asynchronously combine summaries from all batches into a final, comprehensive summary.
    """
    try:
        logging.info(f"Combining {len(batch_responses)} batch responses.")
        combined_summaries = "\n".join(
            [f"Batch {res['batch_id']} Summary:\n{res['response']}" for res in batch_responses]
        )
        messages = [
            {"role": "system", "content": "You are a helpful assistant specializing in insurance documentation analysis."},
            {
                "role": "user",
                "content": f"""
You are tasked with creating a single, high-level overview that combines insights from multiple batches of insurance-related content. Your goal is to provide a clear, concise summary that captures the essence of all the content while maintaining a high-level perspective.

Batch Summaries:
{combined_summaries}

Your tasks:
1. Create a single, coherent high-level overview that:
   - Captures the main themes and patterns across all batches
   - Maintains focus on the big picture rather than specific details
   - Provides a clear understanding of the overall content
2. While keeping the summary high-level, ensure it reflects:
   - Key trends in policy coverage and limitations
   - Common patterns in claims processing
   - General themes in customer inquiries and administrative processes
3. Structure the summary to be:
   - Clear and accessible
   - Focused on major insights
   - Free of unnecessary technical details
4. Use insurance terminology appropriately but keep the language accessible
                """
            }
        ]
        final_summary = await make_openai_request(
            api_key_manager=api_key_manager,
            model="gpt-4.1-nano",
            messages=messages,
            temperature=0.1,
            max_tokens=10000,
            top_p=0.1
        )
        logging.info("Final combination completed successfully.")
        # Ensure we return a tuple (summary, cost)
        if isinstance(final_summary, list) and len(final_summary) == 2:
            return (final_summary[0])
        else:
            return (final_summary, 0)
    except Exception as e:
        logging.error(f"Error combining summaries: {e}")
        return (f"Error combining summaries: {e}", 0)

async def process_descriptions_and_combine_async(descriptions: List[str], batch_size: int = 200) -> Tuple[str, float]:
    """
    Asynchronously process descriptions in parallel and combine summaries.
    Always returns a tuple of (final_summary, prompt_cost).
    """
    if not descriptions:
        logging.error("No descriptions provided for processing.")
        return ("No descriptions available for summarization.", 0)

    logging.info(f"Starting processing for {len(descriptions)} descriptions with batch size {batch_size}.")

    all_batch_responses = []

    # Split descriptions into batches
    batches = [descriptions[i:i + batch_size] for i in range(0, len(descriptions), batch_size)]
    logging.info(f"Split descriptions into {len(batches)} batches.")

    # Create asynchronous tasks for each batch
    tasks = []
    for batch_id, batch in enumerate(batches):
        task = asyncio.create_task(process_descriptions(batch, batch_id))
        tasks.append(task)

    # Gather all batch responses as they complete
    for task in asyncio.as_completed(tasks):
        result = await task
        all_batch_responses.append(result)
        logging.info(f"Batch {result['batch_id']} completed and added to responses.")

    # Combine all batch summaries
    if len(all_batch_responses) > 1:
        final_summary = await combine_results(all_batch_responses)
    elif all_batch_responses:
        # Even if only one batch, ensure it is a tuple (summary, cost)
        single_response = all_batch_responses[0]["response"]
        if isinstance(single_response, list) and len(single_response) == 2:
            final_summary = (single_response[0], single_response[1])
        else:
            final_summary = (single_response, 0)
    else:
        logging.error("No responses generated from the batches.")
        final_summary = ("Failed to generate any summaries.", 0)

    return final_summary

# Example usage
if __name__ == "__main__":
    import sys

    async def main():
        # Example descriptions; replace with your actual data
        descriptions = [
            "Description 1",
            "Description 2",
            "Description 3",
            # Add more descriptions as needed
        ]

        final_summary, cost = await process_descriptions_and_combine_async(descriptions)
        print("Final Summary:")
        print(final_summary)
        print("Cost:", cost)

    # Run the async main function
    asyncio.run(main())
