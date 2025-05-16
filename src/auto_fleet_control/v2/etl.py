from download_cfes import download_cfe_xml_from_drive
from download_nfes import download_nfe_xml_from_drive
from cfe_extract import extract_all_cfe_drive
from nfe_extract import extract_all_nfe_drive
from load import load_data_to_google_sheets
from load import load_data_to_csv
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List

log_file = "log_file.txt"
target_file = "final_etl.csv"
sheet_url = "https://docs.google.com/spreadsheets/d/1crLyYcSAJLBBndty6ssJWcaj1MFhYxjDTelslziXIyI/edit?gid=0#gid=0"

CFE_DRIVE_FOLDER_ID = '1v74MOjBS7bnzWO9gsIaOqOBrL1zm4JEU'
test_cfe_folder_id = '12SbHsVwabhvDaDiC5UQ19X6TBrrxOpz_'
NFE_DRIVE_FOLDER_ID = '10r9kUG398ZidynY9MUFcj6oTnUMbJEWB'
test_nfe_folder_id = '1K7wJvEMO1_MDaf4FGxYwEwkCBrtrFwMn'
CREDENTIALS_FILE = 'key-file.json'

SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def download_xml_from_drive():
    cfe_xml_list = download_cfe_xml_from_drive(CFE_DRIVE_FOLDER_ID, CREDENTIALS_FILE)
    nfe_xml_list = download_nfe_xml_from_drive(NFE_DRIVE_FOLDER_ID, CREDENTIALS_FILE)
    return cfe_xml_list, nfe_xml_list

def extract(cfe_xml_list: List[ET.Element], nfe_xml_list: List[ET.Element]) -> pd.DataFrame:
    """Extract data from Google Sheets and XML files."""
    df_cfe = extract_all_cfe_drive(cfe_xml_list)
    df_nfe = extract_all_nfe_drive(nfe_xml_list)
    return pd.concat([df_cfe, df_nfe], ignore_index=True)

def transform(df: pd.DataFrame) -> pd.DataFrame:

    placa_motorista: Dict[str : str] = {
        "FED9247": "Luiz",
        "GBX2G51": "Mylena",
        "SVD6D88": "Carlos",
        "EPI6184": "Caminhão Baú",
        "BWK2969": "Caminhaão Granel",
        "FZO2960": "Lucas",
    }
    
    placa_carro: Dict[str : str] = {
        "FED9247": "Strada",
        "GBX2G51": "Strada",
        "SVD6D88": "FREIGHTLINER",
        "EPI6184": "Caminhão Baú",
        "BWK2969": "Caminhaão Granel",
        "FZO2960": "Montana",
    }


    df_transformed: pd.DataFrame = df.copy()
    df_transformed['Motorista'] = df_transformed['Placa'].map(placa_motorista).fillna('')
    df_transformed = df_transformed.drop(df_transformed[df_transformed['Fornecedor'].str.contains('BIZUNGA', na=False) & (df_transformed['Tipo'] == 'NFe')].index)
    df_transformed = df_transformed.drop(df_transformed[df_transformed['Fornecedor'].str.contains('CASTELINHO', na=False) & (df_transformed['Tipo'] == 'NFe')].index)

    # Remover linhas que contenham uma placa que não é da Nutri
    placas_carros = list(placa_carro.keys())
    df_transformed = df_transformed[df_transformed['Placa'].str.contains('|'.join(placas_carros), na=False)]


    # Create a boolean mask for rows containing the target words
    contains_combustivel = df_transformed["Descricao"].str.contains("GASOLINA|DIESEL|ETANOL|O\.DIE\.", na=False, case=False)
    # Use the boolean mask to set the 'Historico' column
    df_transformed.loc[contains_combustivel, "Historico"] = "Combustível"
    # Use the same boolean mask to set the 'Categoria' column
    df_transformed.loc[contains_combustivel, "Categoria"] = "Abastecimento"

    
    return df_transformed


def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Downloading XML files from Google Drive...")
    cfe_xml_list, nfe_xml_list = download_xml_from_drive()
    logging.info("XML files downloaded.")
    logging.info("Extracting data from XML files...")
    df = extract(cfe_xml_list, nfe_xml_list)
    logging.info("Data extracted.")
    logging.info("Transforming data...")
    df = transform(df)
    print(df)
    logging.info("Data transformed.")
    logging.info("Loading data to CSV file...")
    load_data_to_csv(target_file, df)
    logging.info("Data loaded to CSV file.")
    logging.info("Loading data to Google Sheets file...")
    load_data_to_google_sheets(sheet_url, df)
    logging.info("Data loaded to Google Sheets file.")
    logging.info("Application finished.")


if __name__ == "__main__":
    main()