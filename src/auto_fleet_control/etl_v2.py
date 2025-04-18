import logging
import pandas as pd
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import re

log_file = "log_file.txt"
target_file = "transformed_data.csv"
xml_path = "C:\\Users\\Henrique\\Dev\\Python\\auto_fleet_control\\notas_xml\\*.xml"

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract_from_xml(xml_path):
    dataframe = pd.DataFrame(columns=["num NF", "valor NF", "descricao produto/servico"])

    # Define the namespace used in the XML
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

    # Parse the XML
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Find all <NFe> tags
    for nfe in root.findall(".//nfe:NFe", ns):
        # Extract nNF
        nNF_elem = nfe.find(".//nfe:ide/nfe:nNF", ns)
        nNF = nNF_elem.text if nNF_elem is not None else None

        # Extract vNF
        vNF_elem = nfe.find(".//nfe:total/nfe:ICMSTot/nfe:vNF", ns)
        vNF = float(vNF_elem.text) if vNF_elem is not None else None

        # Get optional infCpl (for placa and KM)
        infCpl_elem = nfe.find(".//nfe:infAdic/nfe:infCpl", ns)
        infCpl_text = infCpl_elem.text if infCpl_elem is not None else ""

        # Extract "placa" and "KM" using regex
        placa_match = re.search(r"placa\s+([A-Za-z0-9]{7})", infCpl_text)
        km_match = re.search(r"KM\s+(\d+)", infCpl_text)

        placa = placa_match.group(1) if placa_match else None
        km = int(km_match.group(1)) if km_match else None


        # O codigo esta errado - precisa ser corrigido
        # O programa esta adicionando cada produto em um for loop dentro de cada NFe com o valor total da nota
        # Loop through products
        for prod in nfe.findall(".//nfe:det/nfe:prod", ns):
            xProd_elem = prod.find("nfe:xProd", ns)
            xProd = xProd_elem.text if xProd_elem is not None else None

            dataframe = pd.concat([
                dataframe,
                pd.DataFrame([{
                    "num NF": nNF,
                    "valor NF": vNF,
                    "descricao produto/servico": xProd,
                    "placa": placa,
                    "KM": km
                }])
            ], ignore_index=True)

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