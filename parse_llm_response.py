import json
import logging

def parse_llm_response(response):
    """
    Parse the JSON response from the LLM into a structured dictionary of entities.
    Handles inconsistencies in formatting and unexpected prefixes/suffixes.
    """
    parsed_entities = {}

    try:
        # Clean and parse the JSON response
        # Remove potential stray formatting or text artifacts
        response_cleaned = response.strip()
        if response_cleaned.startswith("```json"):
            response_cleaned = response_cleaned[7:]  # Remove the "```json" prefix
        elif response_cleaned.startswith("```"):
            response_cleaned = response_cleaned[3:]  # Remove the "```" prefix
        if response_cleaned.endswith("```"):
            response_cleaned = response_cleaned[:-3]  # Remove the trailing "```"

        # Attempt to parse the cleaned response
        response_json = json.loads(response_cleaned)

        if "chunks" not in response_json:
            logging.error("LLM response does not contain 'chunks' key.")
            return {}

        # Iterate through chunks and extract entity data
        for chunk in response_json["chunks"]:
            chunk_index = chunk.get("chunk_index")
            if chunk_index is None:
                logging.warning("Missing chunk index in LLM response.")
                continue

            # Extract entities
            entities_list = []
            for item in chunk.get("main", []):
                entity = item.get("Entity")
                entity_type = item.get("Type")
                description = item.get("Description")
                step_to_assist_user = item.get("step_to_assist_user")

                if not entity or not entity_type or not description:
                    logging.warning(f"Incomplete entity data in chunk {chunk_index}: {item}")
                    continue

                entities_list.append((
                    entity,
                    entity_type,
                    description,
                    step_to_assist_user
                ))

            parsed_entities[chunk_index] = entities_list

    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse LLM response as JSON: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in parse_llm_response: {e}")

    return parsed_entities
