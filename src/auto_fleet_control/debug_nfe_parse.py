
from datetime import datetime
from pathlib import Path
import re
from typing import List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import io
import xml.etree.ElementTree as ET

import pandas as pd


target_file = "nfe.csv"

def download_nfe_xml_from_drive(folder_id: str, credentials_file: str) -> list:
    """
    Downloads all XML files from a specified Google Drive folder and returns
    their content as a list of ElementTree root objects.

    Args:
        folder_id: The ID of the Google Drive folder.
        credentials_file: Path to your Google Cloud service account credentials JSON file.
                          Defaults to 'path/to/your/credentials.json'.

    Returns:
        A list of ElementTree root objects, where each element corresponds to
        the parsed XML content of a downloaded file. Returns an empty list if
        no XML files are found or if an error occurs.
    """
    try:
        # Authenticate using service account credentials
        creds = Credentials.from_service_account_file(credentials_file,
                                                     scopes=['https://www.googleapis.com/auth/drive'])

        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/xml'",
            fields="files(id, name)").execute()
        items = results.get('files', [])

        xml_data_list = []
        if not items:
            print(f"No XML files found in folder with ID: {folder_id}")
            return xml_data_list

        print(f"Found {len(items)} NFe XML files in the folder.")
        for item in items:
            file_id = item['id']
            file_name = item['name']
            try:
                # Download the file content
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                fh.seek(0)
                xml_content = fh.read()
                # Parse the XML content
                root = ET.fromstring(xml_content)
                # DEBUGGING ROOT
                # print(root)
                # for _ in root:
                #     print(_.tag)
                xml_data_list.append(root)
                print(f"Successfully downloaded NFe: {file_name}")

            except HttpError as error:
                print(f"An error occurred while downloading or parsing {file_name}: {error}")
            except ET.ParseError as error:
                print(f"Error parsing XML in {file_name}: {error}")

        return xml_data_list

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
import logging
# xml_folder = Path("C:/Users/Henrique/Dev/Python/auto_fleet_control/notas_xml")
xml_folder = Path("/Users/henriqueguazzelli/Dev/Python/auto_fleet_control/notas_xml/test.xml")

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
            discount = discount_elem.text if discount_elem is not None else None
            
            if discount is None:
                discount = 0
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

    for element in xml_list:
        try:
            data = extract_from_nfe(element)
            data_frames.append(data)
        except Exception as e:
            logging.error(f"Failed to process {element}: {e}, {"extract_all_nfe_drive"}")

    return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

def load_to_csv(target_file, df):
    df.to_csv(target_file, index=False)

if __name__ == '__main__':
    # Replace with your actual folder ID and credentials file path
    YOUR_DRIVE_FOLDER_ID = '1K7wJvEMO1_MDaf4FGxYwEwkCBrtrFwMn'
    YOUR_CREDENTIALS_FILE = 'key-file.json'

    xml_data_list = download_nfe_xml_from_drive(YOUR_DRIVE_FOLDER_ID, YOUR_CREDENTIALS_FILE)
    # df = extract_from_nfe(xml_data_list[0])
    df = extract_all_nfe_drive(xml_data_list)
    load_to_csv(target_file, df)

    if xml_data_list:
        print("\nSuccessfully downloaded and parsed all XML files.")
        print(f"Number of XML files processed: {len(xml_data_list)}")
        print(f"Data saved to {target_file}")    
    else:
        print("\nNo XML data was downloaded.")