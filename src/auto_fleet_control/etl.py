# from extract_from_sheets import extract_from_google_sheets
import logging
import pandas as pd
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "final.csv"


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract():
    return ""

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Reading data from Google Sheet...")
    df = extract_from_google_sheets(sheet_url)
    logging.info("Data read from Google Sheet.")
    # logging.info("Extracting data from XML files...")
    # extract_from_xml(xml_path)
    # logging.info("XML data extraction complete.")
    # logging.info("Loading data to CSV file...")
    # load_data(target_file, extract_from_xml(xml_path))
    # logging.info("Data loaded to CSV file.")
    # logging.info("Application finished.")
    print(df)

if __name__ == "__main__":
    main()