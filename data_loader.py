# src/data_loader.py

from dotenv import load_dotenv
import os
import io
import re
import time
import zipfile 
import requests
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor 

import dart_fss
import OpenDartReader

load_dotenv()

# Access your API key
API_KEY = os.getenv("OPENDART_API_KEY")

if not API_KEY:
    raise ValueError("OPENDART_API_KEY not found in environment variables. Please set it in a .env file.")

# get all listed corp codes (keys to access opendart) 
# opendart link: https://engopendart.fss.or.kr/guide/detail.do?apiGrpCd=DE001&apiId=AE00004

def initialize_dart_api(api_key: str) -> OpenDartReader:
    """
    Initializes and returns an OpenDartReader instance.
    Sets the API key for dart_fss as well.
    """
    dart_fss.set_api_key(api_key)
    return OpenDartReader(api_key)

# --- 2. Constants for Paths ---
# Define your base data directory relative to the project root
BASE_DATA_DIR = os.path.join('data', 'raw') # All raw data downloads go here
os.makedirs(BASE_DATA_DIR, exist_ok=True) # Ensure the directory exists

# --- 3. Functions for Specific Data Loading Tasks ---

def get_listed_corp_codes(api_key: str, output_dir: str = BASE_DATA_DIR) -> pd.DataFrame:
    """
    Fetches the list of all listed corporation codes from OpenDART API,
    parses the XML, and returns a DataFrame of relevant KOSPI-listed companies.
    Saves the full corp code list to a CSV file.

    Args:
        api_key: Your OpenDART API key.
        output_dir: Directory to save the corpCode.xml and the resulting CSV.

    Returns:
        A pandas DataFrame containing corp_code, corp_name, corp_eng_name, stock_code
        for listed companies (stock_code has 6 digits).
    """
    print("Fetching all corporation codes from OpenDART...")
    url_code = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}'
    try:
        response = requests.get(url_code, stream=True)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching corporation codes: {e}")
        return pd.DataFrame()

    corp_data_path = os.path.join(output_dir, 'dart_corp_data') # Create a sub-dir for raw XML
    os.makedirs(corp_data_path, exist_ok=True)

    xml_zip_path = os.path.join(corp_data_path, 'CORPCODE.zip')
    xml_path = os.path.join(corp_data_path, 'CORPCODE.xml')

    # Save the zip file first, then extract (more robust)
    with open(xml_zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # Unzip and extract CORPCODE.xml
    try:
        with zipfile.ZipFile(xml_zip_path, 'r') as z:
            z.extract('CORPCODE.xml', path=corp_data_path)
        print(f"CORPCODE.xml extracted to {xml_path}")
    except zipfile.BadZipFile:
        print(f"Error: Downloaded file is not a valid zip file. Check API key or response content.")
        return pd.DataFrame()


    # Parse XML
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing CORPCODE.xml: {e}")
        return pd.DataFrame()

    # Collect listed companies (6-digit stock code only)
    corp_list = []
    for corp in root.findall('list'):
        stock_code = corp.findtext('stock_code')
        if stock_code and len(stock_code) == 6: # Filter for 6-digit stock codes (typically KOSPI/KOSDAQ)
            corp_list.append({
                'corp_code': corp.findtext('corp_code'),
                'corp_name': corp.findtext('corp_name'),
                'corp_eng_name': corp.findtext('corp_eng_name'),
                'stock_code': stock_code
            })

    corp_codes_df = pd.DataFrame(corp_list)
    output_filepath = os.path.join(output_dir, 'listed_corp_codes.csv')
    save_df_to_csv(corp_codes_df, output_filepath)
    print(f"Saved {len(corp_codes_df)} listed corporation codes to {output_filepath}")
    return corp_codes_df


def get_kospi_company_info(dart_reader: OpenDartReader, corp_codes_df: pd.DataFrame, output_dir: str = BASE_DATA_DIR) -> pd.DataFrame:
    """
    Fetches detailed company information for KOSPI-listed companies from OpenDartReader.
    Filters for corp_cls 'Y' (Yoo-ga-jeung-kwon Market, i.e., KOSPI).
    Saves the results to a CSV file.

    Args:
        dart_reader: An initialized OpenDartReader instance.
        corp_codes_df: DataFrame containing corp_code and other identifiers.
        output_dir: Directory to save the resulting CSV.

    Returns:
        A pandas DataFrame with detailed information for KOSPI companies.
    """
    print("Fetching detailed company info for KOSPI companies...")
    data = []
    total_corps = len(corp_codes_df)
    for i, row in corp_codes_df.iterrows():
        corp_code = row['corp_code']
        corp_name = row['corp_name']
        try:
            info = dart_reader.company(corp_code)

            # Filter for KOSPI companies ('Y' indicates Yoo-ga-jeung-kwon Market)
            if info and info.get('corp_cls') == 'Y':
                data.append({
                    'corp_name': info.get('corp_name'),
                    'corp_code': info.get('corp_code'),
                    'stock_code': info.get('stock_code'),
                    'ceo_name': info.get('ceo_nm'),
                    'industry_code': info.get('induty_code'),
                    'established_date': info.get('est_dt'),
                    'ir_url': info.get('ir_url')
                    'corp_reg_number': info.get('jurir_no'), 
                    'business_no': info.get('bizr_no')
                })
            # Add a small delay to avoid hitting API rate limits
            time.sleep(0.7) # 10 ms delay
        except Exception as e:
            print(f"Failed to fetch company info for {corp_name} ({corp_code}): {e}")
            continue

    kospi_codes_df = pd.DataFrame(data)
    output_filepath = os.path.join(output_dir, 'kospi_company_info.csv')
    save_df_to_csv(kospi_codes_df, output_filepath)
    print(f"Saved {len(kospi_codes_df)} KOSPI company details to {output_filepath}")
    return kospi_codes_df

def save_df_to_csv(df: pd.DataFrame, file_path: str, index: bool = False):
    """
    Saves a pandas DataFrame to a CSV file.
    Ensures the directory exists before saving.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    try:
        df.to_csv(file_path, index=index)
        # print(f"DataFrame saved to {file_path}") # Can be noisy, uncomment if needed
    except Exception as e:
        print(f"Error saving DataFrame to CSV {file_path}: {e}")

# --- You can add more specific data loading functions here ---
# Example:
# def get_executive_status_report(dart_reader: OpenDartReader, corp_code: str, bsns_year: int, reprt_code: str) -> pd.DataFrame:
#     """Fetches executive status report."""
#     # return dart_reader.executive_status(corp_code=corp_code, bsns_year=bsns_year, reprt_code=reprt_code)
#     pass # Placeholder for actual implementation

# def get_financial_statements(dart_reader: OpenDartReader, corp_code: str, bsns_year: int, reprt_code: str) -> pd.DataFrame:
#     """Fetches financial statements using dart_fss."""
#     # return dart_fss.api.finstate.extract(corp_code=corp_code, bsns_year=bsns_year, reprt_code=reprt_code)
#     pass # Placeholder for actual implementation