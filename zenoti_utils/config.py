import json
import os
import logging
from typing import Dict

def load_zenoti_config(config_path: str = "C:/Users/softwaredeveloper/Desktop/Silkor/Kunal Project/Config/zenoti_centers.json") -> Dict:
    """
    Load Zenoti configuration from a JSON file.
    
    Args:
        config_path (str): Path to the configuration file.
    
    Returns:
        Dict: Configuration dictionary with centers_by_key mapping.
    
    Raises:
        FileNotFoundError: If the config file is not found.
        json.JSONDecodeError: If the config file is invalid JSON.
    """
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.info(f"Loaded configuration from {config_path}")
        return config
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in configuration file {config_path}: {e}", exc_info=True)
        raise