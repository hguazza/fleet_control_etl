import logging
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
import re
# from typing import Optional

log_file = "log_file.txt"
target_file = "transformed_cfe_data.csv"
xml_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/cupons_xml")

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def extract_from_cfe(xml_path: str) -> pd.DataFrame:
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError) as e:
        raise ValueError(f"Failed to parse XML file {xml_path}: {e}")

    nCFe_elem = root.find(".//infCFe/ide/nCFe")
    nCFe = nCFe_elem.text if nCFe_elem is not None else None

    infCpl_elem = root.find(".//infAdic/infCpl")
    infCpl_text = infCpl_elem.text if infCpl_elem is not None else ""

    placa_match = re.search(r"PLACA:\s*([A-Z0-9]{7})", infCpl_text, re.IGNORECASE)
    km_match = re.search(r"KM:\s*(\d+)", infCpl_text, re.IGNORECASE)

    placa = placa_match.group(1).upper() if placa_match else None
    km = int(km_match.group(1)) if km_match else None

    rows = []

    for det in root.findall(".//det"):
        prod_elem = det.find("prod")
        if prod_elem is not None:
            xProd = prod_elem.findtext("xProd")
            vProd_text = prod_elem.findtext("vProd")
            try:
                vProd = float(vProd_text) if vProd_text else None
            except ValueError:
                vProd = None

            rows.append({
                "num CFe": nCFe,
                "valor produto": vProd,
                "descricao produto/servico": xProd,
                "placa": placa,
                "KM": km
            })

    return pd.DataFrame(rows)

def extract_all_cfe(xml_folder: Path):
    all_data = pd.DataFrame()
    xml_files = list(xml_folder.glob("*.xml"))

    if not xml_files:
        logging.warning("Nenhum arquivo XML encontrado.")
        return all_data

    for file_path in xml_files:
        data = extract_from_cfe(file_path)
        all_data = pd.concat([all_data, data], ignore_index=True)

    return all_data

def load_data(target_file, df):
    df.to_csv(target_file, index=False)

def main():
    setup_logging()
    logging.info("Iniciando extração de dados CFe...")
    df = extract_all_cfe(xml_folder)
    logging.info(f"Extração concluída. Total de registros: {len(df)}")
    logging.info("Salvando dados em CSV...")
    load_data(target_file, df)
    logging.info(f"Finalizado com sucesso. Arquivo salvo como: {target_file}")

if __name__ == "__main__":
    main()