import logging
from pathlib import Path
from typing import List
import pandas as pd
from auto_fleet_control.v2.download_nfes import download_nfe_xml_from_drive
from auto_fleet_control.v2.nfe_extract import extract_all_nfe_drive
import xml.etree.ElementTree as ET

target_file = "nfe.csv"

# folder com somente 2 nfes para teste
NFE_DRIVE_FOLDER_ID = '1K7wJvEMO1_MDaf4FGxYwEwkCBrtrFwMn'
CREDENTIALS_FILE = 'key-file.json'

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def download_xml_from_drive():
    nfe_xml_list = download_nfe_xml_from_drive(NFE_DRIVE_FOLDER_ID, CREDENTIALS_FILE)
    return nfe_xml_list

def extract(nfe_xml_list: List[ET.Element]) -> pd.DataFrame:
    """Extract data from Google Sheets and XML files."""
    df_nfe = extract_all_nfe_drive(nfe_xml_list)
    return pd.DataFrame(df_nfe)

def load_data_to_csv(target_file: Path, df: pd.DataFrame):
    df.to_csv(target_file, index=False)

def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Downloading XML files from Google Drive...")
    nfe_xml_list = download_xml_from_drive()
    logging.info("XML files downloaded.")
    logging.info("Extracting data from XML files...")
    df = extract(nfe_xml_list)
    logging.info("Data extracted.")
    logging.info("Loading data to CSV file...")
    load_data_to_csv(target_file, df)
    logging.info("Data loaded to CSV file.")
    logging.info("Application finished.")


if __name__ == "__main__":
    main()