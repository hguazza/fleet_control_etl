import logging
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
import re
# from typing import Optional

log_file = "log_file.txt"
target_file = "transformed_data.csv"
xml_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/notas_xml")

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def extract_from_nfe(xml_path: str) -> pd.DataFrame:
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError) as e:
        raise ValueError(f"Failed to parse XML file {xml_path}: {e}")

    infNFe = root.find(".//nfe:infNFe", ns)
    if infNFe is None:
        raise ValueError("infNFe element not found in XML.")

    nNF_elem = infNFe.find(".//nfe:ide/nfe:nNF", ns)
    nNF = nNF_elem.text if nNF_elem is not None else None

    infCpl_elem = infNFe.find(".//nfe:infAdic/nfe:infCpl", ns)
    infCpl_text = infCpl_elem.text if infCpl_elem is not None else ""

    placa_match = re.search(r"PLACA\s*([A-Z0-9]{7})", infCpl_text, re.IGNORECASE)
    km_match = re.search(r"KM\s*(\d+)", infCpl_text, re.IGNORECASE)

    placa = placa_match.group(1).upper() if placa_match else None
    km = int(km_match.group(1)) if km_match else None

    rows = []

    for prod in infNFe.findall(".//nfe:det/nfe:prod", ns):
        if prod is not None:
            xProd_elem = prod.find("nfe:xProd", ns)
            xProd = xProd_elem.text if xProd_elem is not None else None

            vProd_elem = prod.find("nfe:vProd", ns)
            vProd = vProd_elem.text if vProd_elem is not None else None

            qtde_elem = prod.find("nfe:qCom", ns)
            qtde = qtde_elem.text if qtde_elem is not None else None

            discount_elem = prod.find("nfe:vDesc", ns)
            discount = discount_elem.text if discount_elem is not None else None
            
            valor = (float(vProd) - float(discount)) * float(qtde)
            # try:
            #     vProd = float(vProd_text) if vProd_text else None
            # except ValueError:
            #     vProd = None

            rows.append({
                "num NF": nNF,
                "valor": valor,
                "descricao produto/servico": xProd,
                "placa": placa,
                "KM": km
            })

    return pd.DataFrame(rows)


def extract_all_nfe(xml_folder: Path):
    all_data = pd.DataFrame()
    xml_files = list(xml_folder.glob("*.xml"))

    if not xml_files:
        logging.warning("Nenhum arquivo XML encontrado.")
        return all_data

    for file_path in xml_files:
        data = extract_from_nfe(file_path)
        all_data = pd.concat([all_data, data], ignore_index=True)

    return all_data

def load_data(target_file, df):
    df.to_csv(target_file, index=False)

def main():
    setup_logging()
    logging.info("Iniciando extração de dados NFe...")
    df = extract_all_nfe(xml_folder)
    logging.info(f"Extração concluída. Total de registros: {len(df)}")
    logging.info("Salvando dados em CSV...")
    load_data(target_file, df)
    logging.info(f"Finalizado com sucesso. Arquivo salvo como: {target_file}")

if __name__ == "__main__":
    main()