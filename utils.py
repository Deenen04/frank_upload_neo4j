# utils.py

import logging
import sys

def configure_logging():
    """
    Configures the logging settings for the application, including detailed information 
    such as the file name and line number of the log message.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    
import hashlib

def generate_hash(input_string):
    # Encode the string and generate SHA-256 hash
    hash_object = hashlib.sha256(input_string.encode())
    return hash_object.hexdigest()
