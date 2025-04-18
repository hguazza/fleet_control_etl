import logging
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
import re

log_file = "log_file.txt"
target_file = "transformed_cfe_data.csv"
xml_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/cupons_xml")

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

        infCFe = root.find(".//infCFe")

        if infCFe is None:
            logging.warning(f"infCFe not found in {xml_file}")
            return df

        ide = infCFe.find("ide")
        numero = ide.findtext("nCFe")
        
        total = infCFe.find("total")
        valor_total = float(total.findtext("vCFe")) if total is not None else None

        infAdic = infCFe.find("infAdic")
        infCpl_text = infAdic.findtext("infCpl") if infAdic is not None else ""

        placa_match = re.search(r"PLACA: \s*([A-Za-z0-9]{7})", infCpl_text, re.IGNORECASE)
        km_match = re.search(r"KM: \s*(\d+)", infCpl_text, re.IGNORECASE)

        placa = placa_match.group(1) if placa_match else None
        km = int(km_match.group(1)) if km_match else None

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
        logging.error(f"Erro ao processar o arquivo {xml_file}: {e}")

    return df

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
