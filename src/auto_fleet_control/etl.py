import logging
import pandas as pd
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"
nfe_paths = "C:\\Users\\Henrique\\Dev\\Python\\auto_fleet_control\\notas_xml\\*.xml"
sheet_url = "https://docs.google.com/spreadsheets/d/12qIUsfXlwG_qnb7efwA2aAM2A3_oI3E0BDQA0l0Xfxo/edit?gid=903440851#gid=903440851"


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract_from_google_sheets(sheet_url):
    """Read a Google Sheet and convert it to a DataFrame."""

    # The part of the URL that contains the sheet ID
    sheet_id = sheet_url.split("/d/")[1].split("/edit")[0]
    
    # Construct the CSV export URL
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=903440851"

    try:
        df = pd.read_csv(csv_url)
        print("DataFrame created successfully:")
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure the Google Sheet is publicly accessible.")
    return df

# Extract from CFe


# Extract from NFe


# Extract


def extract_from_xml(xml_file):
    dataframe = pd.DataFrame(columns=["num NF", "valor NF", "descricao produto/servico"])
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for NFe in root:
        nNF = NFe.find("nNF").text
        vNF = float(NFe.find("vNF").text)
        xProd = float(NFe.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"num NF":nNF, "valor NF":vNF, "descricao produto/servico":xProd}])], ignore_index=True)
    return dataframe

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