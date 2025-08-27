# chunk_text.py

import asyncio
import logging
import re # re is used for splitting by "break" case-insensitively
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Max number of non-empty lines for a section to be considered "short"
# and potentially carried over if it's at the end of a non-final page.
# The example "Subscriber" is 1 line. Setting to 1 makes it less aggressive.
# Set to 2 if titles with one immediate sub-line should also be carried.
_MAX_LINES_FOR_HANGING_SECTION = 1


async def chunk_text_by_rules_async(all_document_text: str) -> Tuple[List[str], List[Dict]]:
    """
    Processes text from a single string into chunks based on 'break' delimiters.

    :param all_document_text: A single string containing the entire document text.
    :return: A tuple containing a list of all chunks (str)
             and their corresponding metadata (List[Dict]).
             Metadata includes 'chunk_index' and 'title'.
    """
    chunks_list = []
    metadata_list = []
    
    if not all_document_text or not all_document_text.strip():
        return [], []

    # Split the entire document content by "break" (case-insensitive, whole word)
    parts_between_breaks = re.split(r'\bbreak\b', all_document_text, flags=re.IGNORECASE)
    
    # Process each part as a potential chunk
    for chunk_idx, single_chunk_content in enumerate(parts_between_breaks):
        stripped_content = single_chunk_content.strip()
        if stripped_content:
            # The first non-empty line can be considered the title for metadata
            title_lines = stripped_content.split('\n')
            title = title_lines[0].strip() if title_lines else "Untitled Chunk"
            
            chunks_list.append(stripped_content)
            metadata_list.append({
                "chunk_index": chunk_idx + 1, # Use a sequential index as identifier
                "title": title
            })
            logging.debug(f"Added chunk {chunk_idx + 1} with title: '{title}'")

    logging.info(f"Total chunks created: {len(chunks_list)}")
    return chunks_list, metadata_list

# Example usage
if __name__ == "__main__":
    # Set logging level to DEBUG to see detailed processing steps for tests
    # logging.getLogger().setLevel(logging.DEBUG)

    async def main():
        # Example 1: Combined content from previous user case
        all_text_user_case = """
Who qualifies as a dependent under the member's specific plan
If a dependent is currently enrolled or eligible
Age-out rules and continuation options (e.g., COBRA)
Where to Find:
Enrollment summary in HR or benefits portal
Carrier or employer eligibility guidelines
ID card (shows enrolled names in some cases)
break
Subscriber
Definition:
The subscriber is the primary person who holds the insurance policy. In employer-sponsored
plans, this is typically the employee. All plan documents, ID cards, and billing records are
associated with the subscriber.
Clarifying Steps:
Support should confirm:
Whether the person calling is the subscriber or a dependent
That subscriber permission is documented if discussing details with someone else
That all plan communications and ID numbers are under the subscriber's name
Where to Find:
"""
        print("\n--- Testing Combined User Case ---")
        chunks_list, metadata_list = await chunk_text_by_rules_async(all_text_user_case)
        for chunk_idx, (chunk, meta) in enumerate(zip(chunks_list, metadata_list)):
            print(f"\nChunk {chunk_idx+1}: Index {meta['chunk_index']} - Title: {meta['title']}\n---\n{chunk}\n---")

        # Example 2: Combined content from original example
        all_text_original = """
Deductible
Definition:
A deductible is the amount a member must pay for covered healthcare services before the insurance plan begins contributing toward those costs. Deductibles typically reset each calendar or plan year and apply to major services such as hospital visits, surgeries, or diagnostic procedures. Once this threshold is met, coinsurance or copays apply depending on the plan structure.
Clarifying Steps:
Customer support staff should confirm:
If the deductible is individual or family-based
Whether it's separate or combined for medical and pharmacy
The amount accumulated so far this year (using carrier portal)
Where to Find:
Summary of Benefits and Coverage (SBC)
Carrier or employer benefit portal
Member ID card (often shows individual/family deductible amounts)
break
Copay
Definition:
A copay is a fixed dollar amount that a member pays at the time of service for specific healthcare services such that primary care visits, specialist appointments, or prescriptions. It applies even if the deductible hasn't been met and does not contribute to the deductible but may count toward the out-of-pocket maximum.
Clarifying Steps:
Customer support should:
Confirm the exact copay amount per service type
Clarify that copays apply regardless of deductible status
Advise the member that multiple copays may apply in one visit (e.g., office visit + lab work)
Where to Find:
Member ID card (lists copay amounts by service type)
SBC and plan brochure
Carrier portal
break"""
        print("\n--- Testing Original Example (Combined Text) ---")
        chunks_list_orig, metadata_list_orig = await chunk_text_by_rules_async(all_text_original)
        for chunk_idx, (chunk, meta) in enumerate(zip(chunks_list_orig, metadata_list_orig)):
            print(f"\nChunk {chunk_idx+1}: Index {meta['chunk_index']} - Title: {meta['title']}\n---\n{chunk}\n---")

        # Example 3: Short title carried over multiple empty pages (simulate as combined text)
        all_text_empty_carry = "Content on Page 1\nbreak\nFloating Title\n\n\n\nFinally, content for the floating title."
        print("\n--- Testing Carry Over Multiple Empty Sections (Combined Text) ---")
        chunks_list_empty, metadata_list_empty = await chunk_text_by_rules_async(all_text_empty_carry)
        for chunk_idx, (chunk, meta) in enumerate(zip(chunks_list_empty, metadata_list_empty)):
            print(f"\nChunk {chunk_idx+1}: Index {meta['chunk_index']} - Title: {meta['title']}\n---\n{chunk}\n---")

        # Example 4: Text starts with break
        all_text_starts_break = "break\nFirst actual title\nContent here.\nAnother section."
        print("\n--- Testing Text Starts with Break ---")
        chunks_list_sb, metadata_list_sb = await chunk_text_by_rules_async(all_text_starts_break)
        for chunk_idx, (chunk, meta) in enumerate(zip(chunks_list_sb, metadata_list_sb)):
            print(f"\nChunk {chunk_idx+1}: Index {meta['chunk_index']} - Title: {meta['title']}\n---\n{chunk}\n---")

        # Example 5: Text with only break
        all_text_only_break = "Content Section 1\nbreak\nbreak\nContent Section 3"
        print("\n--- Testing Text with only Break ---")
        chunks_list_ob, metadata_list_ob = await chunk_text_by_rules_async(all_text_only_break)
        for chunk_idx, (chunk, meta) in enumerate(zip(chunks_list_ob, metadata_list_ob)):
            print(f"\nChunk {chunk_idx+1}: Index {meta['chunk_index']} - Title: {meta['title']}\n---\n{chunk}\n---")

    asyncio.run(main())