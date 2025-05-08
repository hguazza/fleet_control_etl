import logging
from pathlib import Path
from typing import List
import pandas as pd
import xml.etree.ElementTree as ET
import re
from datetime import datetime
# from typing import Optional

log_file = "log_file.txt"
target_file = "nfe.csv"

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def extract_from_nfe(root: ET.Element) -> pd.DataFrame:
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

    infNFe = root.find(".//nfe:infNFe", ns)
    if infNFe is None:
        raise ValueError("infNFe element not found in XML.")

    nNF_elem = infNFe.find(".//nfe:ide/nfe:nNF", ns)
    nNF = nNF_elem.text if nNF_elem is not None else None

    data_elem = infNFe.find(".//nfe:ide/nfe:dhEmi", ns)
    # data = data_elem.text[:10].replace("-", "/") if data_elem is not None else None
    data_text = data_elem.text[:10]
    date = datetime.strptime(data_text, "%Y-%m-%d")
    date = date.strftime("%d/%m/%Y")

    fornecedor_elem = infNFe.find(".//nfe:emit/nfe:xNome", ns)
    fornecedor = fornecedor_elem.text if fornecedor_elem is not None else None

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
            discount = discount_elem.text if discount_elem is not None else 0
            
            valor = (float(vProd) - float(discount)) * float(qtde)
            
            rows.append({
                "Data": date,
                "Numero": nNF,
                "Valor": valor,
                "Descricao": xProd,
                "Placa": placa,
                "KM": km,
                "Fornecedor": fornecedor,
                "Motorista": "",
                "Histórico": "",
                "Categoria": "NFE"
            })

    return pd.DataFrame(rows)


def extract_all_nfe_drive(xml_list: List[ET.Element]) -> pd.DataFrame:
    data_frames = []

    if not xml_list:
        logging.warning("No NFe XML files found.")
        return pd.DataFrame()

    for file in xml_list:
        try:
            data = extract_from_nfe(file)
            data_frames.append(data)
        except Exception as e:
            logging.error(f"Failed to process {file}: {e}, {"extract_all_nfe_drive"}")

    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()


def load_to_csv(target_file, df):
    df.to_csv(target_file, index=False)


def main():
    setup_logging()
    logging.info("Iniciando extração de dados NFe...")
    df = extract_all_nfe(xml_list)
    logging.info(f"Extração concluída. Total de registros: {len(df)}")
    logging.info("Salvando dados em CSV...")
    load_to_csv(target_file, df)
    logging.info(f"Finalizado com sucesso. Arquivo salvo como: {target_file}")

if __name__ == "__main__":
    main()