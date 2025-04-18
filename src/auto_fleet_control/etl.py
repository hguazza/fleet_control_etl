import logging
import pandas as pd
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"
xml_path = "C:\\Users\\Henrique\\Dev\\Python\\auto_fleet_control\\notas_xml\\test.xml"

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract_from_xml(xml_path):
    dataframe = pd.DataFrame(columns=["num NF", "valor NF", "descricao produto/servico"])
    tree = ET.parse(xml_path)
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
    logging.info("Extracting data from XML files...")
    extract_from_xml(xml_path)
    logging.info("XML data extraction complete.")
    logging.info("Loading data to CSV file...")
    load_data(target_file, extract_from_xml(xml_path))
    logging.info("Data loaded to CSV file.")
    logging.info("Application finished.")

if __name__ == "__main__":
    main()