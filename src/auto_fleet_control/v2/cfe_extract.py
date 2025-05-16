import logging
from pathlib import Path
from typing import List
import pandas as pd
import xml.etree.ElementTree as ET
import re
from datetime import datetime
# from typing import Optional


now = datetime.now()
today = now.strftime("%Y-%m-%d")

log_file = "log_file.txt"
target_file = "cfe.csv"

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def extract_from_cfe(root: ET.Element) -> pd.DataFrame:

    data_elem = root.find(".//infCFe/ide/dEmi")
    date = data_elem.text if data_elem is not None else None
    date = pd.to_datetime(date, format="%Y%m%d")
    date = date.strftime('%Y-%m-%d')

    nCFe_elem = root.find(".//infCFe/ide/nCFe")
    nCFe = nCFe_elem.text if nCFe_elem is not None else None

    fornecedor_elem = root.find(".//infCFe/emit/xNome")
    fornecedor = fornecedor_elem.text if fornecedor_elem is not None else None

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
            vProd = float(vProd_text) if vProd_text else 0
            vDesconto_text = prod_elem.findtext("vDesc")
            vDesconto = float(vDesconto_text) if vDesconto_text else 0
            vProd -= vDesconto

            rows.append({
                "Data": date,
                "Numero": nCFe,
                "Tipo": "CFe",
                "Valor": vProd,
                "Descricao": xProd,
                "Fornecedor": fornecedor,
                "Historico": "",
                "Categoria": "",
                "Placa": placa,
                "Veiculo": "",
                "KM": km,
                "Motorista": "",
            })

    return pd.DataFrame(rows)

def extract_all_cfe_drive(xml_list: List[ET.Element]) -> pd.DataFrame:
    data_frames = []

    if not xml_list:
        logging.warning("No CFe XML files found.")
        return pd.DataFrame()

    for file in xml_list:
        try:
            data = extract_from_cfe(file)
            data_frames.append(data)
        except Exception as e:
            logging.error(f"Failed to process {file}: {e}")

    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

def load_data(target_file, df):
    df.to_csv(target_file, index=False)

def main():
    setup_logging()
    logging.info("Iniciando extração de dados CFe...")
    df = extract_all_cfe_drive(xml_list)
    logging.info(f"Extração concluída. Total de registros: {len(df)}")
    logging.info("Salvando dados em CSV...")
    load_data(target_file, df)
    logging.info(f"Finalizado com sucesso. Arquivo salvo como: {target_file}")

if __name__ == "__main__":
    main()