from extract_from_sheets import extract_from_google_sheets
from cfe_improved_v2 import extract_all_cfe
from nfe_improved_v2 import extract_all_nfe
from load import load_data_to_google_sheets
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

log_file = "log_file.txt"
target_file = "final.csv"
sheet_url = "https://docs.google.com/spreadsheets/d/1HtXXEe58zNsG4I3k7qZLBrHJUlmruJEj9jYNffV6w0c/edit?gid=0#gid=0"
cfe_fodler = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/cupons_xml")
nfe_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/notas_xml")

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract(sheet_url, cfe_folder, nfe_folder):
    """Extract data from Google Sheets and XML files."""
    df = extract_from_google_sheets(sheet_url)
    df_cfe = extract_all_cfe(cfe_folder)
    df_nfe = extract_all_nfe(nfe_folder)
    return pd.concat([df, df_cfe, df_nfe], ignore_index=True)

def load_data_to_csv(target_file, df):
    df.to_csv(target_file, index=False)

def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Extracting data...")
    df = extract(sheet_url, cfe_fodler, nfe_folder)
    logging.info("Data extracted.")
    logging.info("Loading data to CSV file...")
    load_data_to_csv(target_file, df)
    logging.info("Data loaded to CSV file.")
    logging.info("Loading data to Google Sheets file...")
    load_data_to_google_sheets(sheet_url, df)
    logging.info("Application finished.")


if __name__ == "__main__":
    main()