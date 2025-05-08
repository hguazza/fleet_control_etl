from gsheets_extract import extract_from_google_sheets
from cfe_extract import extract_all_cfe
from nfe_extract import extract_all_nfe
from load import load_data_to_google_sheets
from download_cfes import download_cfe_xml_from_drive
from cfe_extract_drive import extract_all_cfe_drive
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict

log_file = "log_file.txt"
target_file = "etl.csv"
sheet_url = "https://docs.google.com/spreadsheets/d/1HtXXEe58zNsG4I3k7qZLBrHJUlmruJEj9jYNffV6w0c/edit?gid=0#gid=0" # to load data

# cfe_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/cupons_xml")
cfe_folder = Path("/Users/henriqueguazzelli/Dev/Python/auto_fleet_control/cupons_xml")
# nfe_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/notas_xml")
nfe_folder = Path("/Users/henriqueguazzelli/Dev/Python/auto_fleet_control/notas_xml")

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract(sheet_url: str, cfe_folder: str, nfe_folder: str) -> pd.DataFrame:
    """Extract data from Google Sheets and XML files."""
    df = extract_from_google_sheets(sheet_url)
    df_cfe = extract_all_cfe(cfe_folder)
    df_nfe = extract_all_nfe(nfe_folder)
    return pd.concat([df, df_cfe, df_nfe], ignore_index=True)

def transform(df: pd.DataFrame) -> pd.DataFrame:

    placa_motorista: Dict[str : str] = {
    "FED9247": "Luiz",
    "GBX2G51": "Mylena",
    "SVD6D88": "Carlos",
    "EPI6184": "Caminhão Baú",
    "BWK2969": "Caminhaão Granel"
}

    df_transformed: pd.DataFrame = df.copy()
    df_transformed['Motorista'] = df_transformed['Placa'].map(placa_motorista).fillna('')
    
    return df_transformed

def load_data_to_csv(target_file: Path, df: pd.DataFrame):
    df.to_csv(target_file, index=False)

def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Extracting data...")
    df = extract(sheet_url, cfe_folder, nfe_folder)
    logging.info("Data extracted.")
    logging.info("Transforming data...")
    df = transform(df)
    logging.info("Data transformed.")
    logging.info("Loading data to CSV file...")
    load_data_to_csv(target_file, df)
    logging.info("Data loaded to CSV file.")
    # logging.info("Loading data to Google Sheets file...")
    # df = df.fillna('')
    # load_data_to_google_sheets(sheet_url, df)
    logging.info("Application finished.")


if __name__ == "__main__":
    main()