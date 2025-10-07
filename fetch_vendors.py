import logging
from typing import List, Dict
from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
import requests
import os
import json
from zenoti_utils.config import load_zenoti_config  # Import the config loader

# Configure logging
def set_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# Function to export data to a single Excel file with multiple sheets
def export_orgs_to_excel(data: Dict[str, DataFrame], output_dir: str, report_name: str, date_str: str):
    """
    Export each organization's DataFrame to a separate sheet in a single Excel file.
    
    Args:
        data: Dictionary of organization keys and their DataFrames.
        output_dir: Directory to save the Excel file.
        report_name: Name of the report for filename.
        date_str: Date string for the filename (e.g., '2025-10-06').
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{report_name}_{date_str}.xlsx")
    
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for org, df in data.items():
                df.to_excel(writer, sheet_name=org, index=False)
                logging.info(f"Wrote {org} data to sheet '{org}' in {output_file}")
        logging.info(f"Saved Excel file: {output_file}")
    except Exception as e:
        logging.error(f"Error saving to Excel file {output_file}: {e}", exc_info=True)

# Function to fetch vendor data for a specific organization
def fetch_vendors(org: str, start_date: datetime, end_date: datetime, config: Dict) -> Dict:
    """
    Fetch vendor data from Zenoti API for a specific organization.
    
    Args:
        org: Organization key (e.g., 'lebanon').
        start_date: Start date of the data period.
        end_date: End date of the data period.
        config: Configuration dictionary with centers_by_key and org_to_api_key mappings.
    
    Returns:
        Dictionary containing the API response.
    """
    # Get API key from config
    api_key = config.get('org_to_api_key', {}).get(org.lower())
    if not api_key:
        logging.error(f"No API key found for organization: {org}")
        return {}
    
    base_url = "https://api.zenoti.com/v1/vendors"
    headers = {
        'Authorization': f'apikey {api_key}'
    }
    page = 1
    size = 100
    all_vendors = []
    
    logging.debug(f"Fetching vendors for {org} from {start_date} to {end_date}")
    
    while True:
        # Assuming the API accepts start_date and end_date; adjust if needed
        url = f"{base_url}?page={page}&size={size}&start_date={start_date.strftime('%Y-%m-%d')}&end_date={end_date.strftime('%Y-%m-%d')}"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Extract vendor records (adjust key based on actual API response)
            vendors = data.get('vendors', []) or data.get('data', [])
            if not vendors:
                logging.debug(f"No more data for {org} on page {page}")
                break
                
            # Map center IDs to names using config
            centers = config['centers_by_key'].get(api_key, {})
            for vendor in vendors:
                if 'center_id' in vendor:
                    vendor['center_name'] = centers.get(vendor['center_id'], vendor['center_id'])
                    # Remove center_id to avoid duplication in output
                    vendor.pop('center_id', None)
            
            all_vendors.extend(vendors)
            page += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching page {page} for {org}: {e}", exc_info=True)
            return {}
    
    return {'vendors': all_vendors}

def get_vendors_report(organizations: List[str] | str,
                       start_date: datetime,
                       end_date: datetime) -> Dict[str, DataFrame]:
    """
    Fetch and format vendor data for one or multiple organizations.

    Args:
        organizations (List[str] | str): Org key(s) such as ["lebanon", "kuwait"] or just "lebanon".
        start_date (datetime): Start date of the report period.
        end_date (datetime): End date of the report period.

    Returns:
        Dict[str, DataFrame]: Dictionary with org keys and cleaned DataFrames.
    """
    logging.info(f"Fetching vendors report for organizations: {organizations} from {start_date} to {end_date}")
    
    # Load configuration
    config = load_zenoti_config()
    
    if isinstance(organizations, str):
        organizations = [organizations]
        logging.debug(f"Converted organizations to list: {organizations}")

    # -------- Validate organizations --------
    valid_orgs = [org for org in organizations if org.lower() in config.get('org_to_api_key', {})]
    if not valid_orgs:
        logging.error("No valid organizations found in config.")
        raise ValueError("No valid organizations found in config.")
    logging.debug(f"Valid organizations: {valid_orgs}")

    # -------- Fetch raw data --------
    raw_data = {org: fetch_vendors(org, start_date, end_date, config) for org in valid_orgs}
    logging.info(f"Fetched vendors data for {len(raw_data)} organizations")

    # -------- Validate keys --------
    for org, data in raw_data.items():
        if not data or "vendors" not in data.keys():
            logging.error(f"No vendors data found for {org}.", exc_info=True)
            raise ValueError(f"No vendors data found for {org}.")

    # -------- Extract main data --------
    extracted_data = {org: raw_data[org]["vendors"] for org in valid_orgs if "vendors" in raw_data[org].keys()}
    logging.debug(f"Extracted vendors data for {len(extracted_data)} organizations")

    # -------- Convert to DataFrames --------
    dataframes = {org: DataFrame(df) for org, df in extracted_data.items()}
    logging.info("Loaded raw data into DataFrames.")

    # -------- Normalize work_phone column --------
    for org, dataframe in dataframes.items():
        if 'work_phone' in dataframe.columns:
            def normalize_phone(phone):
                try:
                    # Ensure phone is a dict; if not, return as-is or empty string
                    if not isinstance(phone, dict):
                        return str(phone) if phone else ''
                    phone_code = phone.get('phone_code', 0)
                    number = phone.get('number', '')
                    # If number is empty, return empty string
                    if not number:
                        return ''
                    # If phone_code is non-zero and valid, combine with number
                    if phone_code and phone_code != 0:
                        return f"{phone_code}{number}"
                    # Otherwise, use number only
                    return number
                except Exception as e:
                    logging.warning(f"Error normalizing phone {phone}: {e}")
                    return ''
            
            # Apply normalization to work_phone column
            dataframe['work_phone'] = dataframe['work_phone'].apply(normalize_phone)
            logging.debug(f"Normalized work_phone column for {org}")

    # -------- Data Cleaning & Formatting --------
    for org, dataframe in dataframes.items():
        # Convert date columns (if any, e.g., 'created_date')
        # Example:
        # if 'created_date' in dataframe.columns:
        #     dataframe["created_date"] = pd.to_datetime(dataframe["created_date"])
        #     dataframe["created_date"] = dataframe["created_date"].dt.strftime("%#m/%#d/%Y")

        # No column_index or column_order: Keep all columns from API response
        # Rename work_phone for clarity
        if 'work_phone' in dataframe.columns:
            dataframe.rename(columns={'work_phone': 'Work Phone'}, inplace=True)
        
        # Ensure center_name is included
        if 'center_name' not in dataframe.columns:
            dataframe['center_name'] = ''

        dataframes[org] = dataframe

    return dataframes

if __name__ == "__main__":
    set_logging()
    # Load organizations from config
    config = load_zenoti_config()
    _organizations = list(config.get('org_to_api_key', {}).keys())  # Get all orgs from config
    _end_date = datetime.today() - timedelta(days=1)
    _start_date = _end_date.replace(day=1)

    # Generate the report
    _data = get_vendors_report(_organizations, _start_date, _end_date)

    # Export result to Excel
    name_ = "vendors"
    date_str = _end_date.strftime('%Y-%m-%d')
    output_dir = f"C:/Users/softwaredeveloper/Desktop/Silkor/Kunal Project/Data/{name_}/{date_str}"
    export_orgs_to_excel(_data, output_dir, name_, date_str)