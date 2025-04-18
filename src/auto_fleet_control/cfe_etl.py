import logging
import pandas as pd
import xml.etree.ElementTree as ET
from glob import glob
import re

log_file = "log_file.txt"
target_file = "transformed_cfe_data.csv"
xml_path = "C:\\Users\\Henrique\\Dev\\Python\\auto_fleet_control\\cupons_xml\\*.xml"

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def extract_from_cfe(xml_file):
    df = pd.DataFrame(columns=["CFe Numero", "Valor Total", "Descrição Item", "Placa", "KM"])

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Main tag may be <CFe> or <CFeCFe>
        infCFe = root.find(".//infCFe")

        if infCFe is None:
            logging.warning(f"infCFe not found in {xml_file}")
            return df

        ide_elem = infCFe.find("ide")
        numero = ide_elem.findtext("nCFe")
        
        total_elem = infCFe.find("total")
        valor_total = float(total_elem.findtext("vCFe")) if total_elem is not None else None

        # Optional additional info
        infAdic_elem = infCFe.find("infAdic")
        infCpl_text = infAdic_elem.findtext("infCpl") if infAdic_elem is not None else ""

        placa_match = re.search(r"PLACA\s*([A-Z]{3}\d{4})", infCpl_text, re.IGNORECASE)
        km_match = re.search(r"KM\s*(\d+)", infCpl_text, re.IGNORECASE)

        placa = placa_match.group(1) if placa_match else None
        km = int(km_match.group(1)) if km_match else None

        # Loop through items
        for det in infCFe.findall("det"):
            prod = det.find("prod")
            if prod is not None:
                descricao = prod.findtext("xProd")

                df = pd.concat([
                    df,
                    pd.DataFrame([{
                        "CFe Numero": numero,
                        "Valor Total": valor_total,
                        "Descrição Item": descricao,
                        "Placa": placa,
                        "KM": km
                    }])
                ], ignore_index=True)

    except Exception as e:
        logging.error(f"Error processing file {xml_file}: {e}")

    return df

def extract_all_cfe(xml_path):
    all_data = pd.DataFrame()
    for file in glob(xml_path):
        data = extract_from_cfe(file)
        all_data = pd.concat([all_data, data], ignore_index=True)
    return all_data

def load_data(target_file, df):
    df.to_csv(target_file, index=False)

def main():
    setup_logging()
    logging.info("Iniciando extração de dados CFe...")
    df = extract_from_cfe(xml_path)
    logging.info("Extração concluída.")
    logging.info("Salvando dados em CSV...")
    load_data(target_file, df)
    logging.info("Finalizado com sucesso.")

if __name__ == "__main__":
    main()
