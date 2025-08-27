# analyze_relationships.py

import logging
from apiChatCompletion import APIKeyManager, make_openai_request

async def analyze_chunks_with_llm_async(*chunks):
    """
    Asynchronously analyze multiple chunks with OpenAI using alternating API keys.

    Accepts up to 10 chunks. Ignores None values.
    """
    # Start the fixed structure for the prompt
    prompt = prompt = """
    Analyze each of the following text sections and extract relevant entities along with their types. For each entity, provide a single compact search descriptor string for embeddings. The descriptor must:
    - Use keywords/short phrases only, separated by semicolons
    - Include main intent, alternative phrasings, key domain terms (e.g., insurance types), and routing keywords if present
    - Exclude filler, steps, or greetings
    - Be concise but information-rich for similarity search

    You can assign multiple categories to a single entity if it fits into more than one category. The categories are as follows:
    - **Entity Categories**: Each entity must be categorized into one or more of these insurance-specific categories:
      1. Policy Question : Questions related to a member's existing plan or benefits. These involve clarification about what's included in the plan, plan documents, ID cards, deductibles, out-of-pocket costs, provider networks, etc.
      2. Claims Question : Questions that arise after a medical event or service has occurred. These typically involve billing, denials, reimbursements, appeals, or status of submitted claims.
      3. Sales Question : Questions that relate to buying, changing, comparing, or canceling a plan. These may also involve quotes, eligibility checks, special enrollment, or life insurance product inquiries.
      4. General Question (Default Tag): Any non-specific, informational, or uncategorizable inquiry. This includes office hours, how to speak with an agent, portal access help, or vague requests. It also applies when a question doesn't fit clearly into Policy, Claims, or Sales.

    - **Entity Types and Descriptions**: 
      - Ensure that entities and their types are precise and relevant to insurance operations
      - Description should follow the search descriptor rules (compact keyword-only string for embeddings, separated by semicolons)
      - Include relevant policy numbers, claim IDs, or other reference numbers when present
      - Highlight any time-sensitive information or deadlines
      - Note any specific coverage details or limitations mentioned

    - **Completeness**: 
      - Do not leave any chunks without categorization or description unless the content is empty
      - If needed, relate the chunk to other chunks to maintain context
      - Ensure each chunk's information is independently meaningful
      - Include multiple categories if the content spans different insurance aspects
      - Only skip categorization if the content is empty. If you don't know the category, use the default tag "General Question"
      - Each chunk must also include a "step_to_assist_user" field. This field must clearly list, in **full natural sentences**, the step-by-step way a support agent or system would guide the user to resolve their request. 
      - Do not only output abstract instructions—write them as if they will be shown directly to the user (e.g., "First, check your plan ID on your insurance card. Next, log into the portal and go to 'Claims Status'. If you cannot find the information, call customer support.").

    - **Empty or Irrelevant Sections**: If a section is empty or irrelevant, ignore it and return an empty 'main' array for that section.

    Return the response in this JSON format (note: only keep the following fields):
    {
        "chunks": [
            {
                "chunk_index": <index>,
                "main": []
            }
        ]
    }
    or
    {
        "chunks": [
            {
                "chunk_index": <index>,
                "main": [
                    {
                        "Entity": "string",
                        "Type": "string",
                        "Description": "string",
                        "step_to_assist_user": "string"
                    }
                ]
            }
        ]
    }

    Text sections:
    """



    # Dynamically add chunks to the prompt if they are not None
    for i, chunk in enumerate(chunks, start=1):
        if chunk is not None:
            prompt += f"Section {i}: \"{chunk}\"\n"
            
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": prompt}
    ]

    try:
        # Get the next API key
        api_key_manager = APIKeyManager.from_env("ANALYZE_RELATIONSHIPS")

        response = await make_openai_request(
            api_key_manager=api_key_manager,
            model="gpt-4.1-mini",
            messages=messages,
            temperature=0,
            max_tokens=2500,
            top_p=0
        )

        return response

    except Exception as e:
        logging.error(f"Error with API call: {e}")
        return None
