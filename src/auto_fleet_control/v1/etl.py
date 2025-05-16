from gsheets_extract import extract_from_google_sheets
from cfe_extract import extract_all_cfe
from nfe_extract import extract_all_nfe
from load import load_data_to_google_sheets
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict

log_file = "log_file.txt"
target_file = "etl.csv"
sheet_url = "https://docs.google.com/spreadsheets/d/1HtXXEe58zNsG4I3k7qZLBrHJUlmruJEj9jYNffV6w0c/edit?gid=0#gid=0" # to load data

spreadsheet_key = "1crLyYcSAJLBBndty6ssJWcaj1MFhYxjDTelslziXIyI"

# credentials_path = os.getenv("google_sheet_credentials_file")
credentials_path = "key-file.json"

# Define the scope for Google Sheets API
SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

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

def extract(cfe_folder: str, nfe_folder: str) -> pd.DataFrame:
    """Extract data from Google Sheets and XML files."""
    # df = extract_from_google_sheets(sheet_url)
    df_cfe = extract_all_cfe(cfe_folder)
    df_nfe = extract_all_nfe(nfe_folder)
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

    # df_transformed['Data'] = pd.to_datetime(df_transformed['Data'])
    # df_transformed['mesAno'] = df_transformed['Data'].dt.strftime('%m-%Y')
    # df_transformed['Data'] = df_transformed['Data'].astype(str)

    return df_transformed

def load_data_to_csv(target_file: Path, df: pd.DataFrame):
    df.to_csv(target_file, index=False)

def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Extracting data...")
    df = extract(cfe_folder, nfe_folder)
    logging.info("Data extracted.")
    logging.info("Transforming data...")
    print(df.shape)
    df = transform(df)
    print(df.shape)
    logging.info("Data transformed.")
    logging.info("Loading data to CSV file...")
    load_data_to_csv(target_file, df)
    logging.info("Data loaded to CSV file.")
    logging.info("Loading data to Google Sheets file...")
    df = df.fillna('')
    load_data_to_google_sheets(spreadsheet_key, df)
    logging.info("Application finished.")


if __name__ == "__main__":
    main()