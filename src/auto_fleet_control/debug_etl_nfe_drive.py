import io
from download_nfes import download_nfe_xml_from_drive
from nfe_extract_drive import extract_all_nfe_drive
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List

log_file = "log_file.txt"
target_file = "final_etl.csv"

NFE_DRIVE_FOLDER_ID = '1K7wJvEMO1_MDaf4FGxYwEwkCBrtrFwMn'

CREDENTIALS_FILE = 'key-file.json'


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

xml_path = Path("/Users/henriqueguazzelli/Dev/Python/auto_fleet_control/notas_xml/test.xml")
def extract_from_nfe(xml_path: str):  
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError) as e:
        raise ValueError(f"Failed to parse XML file {xml_path}: {e}")

    infNFe = root.find(".//nfe:infNFe", ns)
    if infNFe is None:
        raise ValueError("infNFe element not found in XML.")
    
    return infNFe


def download_nfe_xml_from_drive(folder_id: str, credentials_file: str) -> list:
    """Downloads NFe XML files from a Google Drive folder and extracts the infNFe elements.

    Args:
        folder_id: The ID of the Google Drive folder.
        credentials_file: Path to the service account credentials JSON file.

    Returns:
        A list of infNFe elements, or an empty list if no XML files are found or an error occurs.
    """
    xml_data_list = []
    try:
        # Authenticate using service account credentials
        creds = Credentials.from_service_account_file(credentials_file,
                                                     scopes=['https://www.googleapis.com/auth/drive'])

        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/xml'",
            fields="files(id, name)").execute()
        items = results.get('files', [])

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


                # Parse the XML content and extract infNFe
                # ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
                try:
                    root = ET.fromstring(xml_content)
                except (ET.ParseError, FileNotFoundError) as e:
                    raise ValueError(f"Failed to parse XML file {xml_path}: {e}")

                infNFe = root.findall(".//nfe:infNFe")
                if infNFe is None:
                    raise ValueError("infNFe element not found in XML.")
                
                return infNFe

            except HttpError as error:
                print(f"An error occurred while downloading {file_name}: {error}")
            except ET.ParseError as error:
                print(f"Error parsing XML in {file_name}: {error}")
            except Exception as e:
                print(f"An unexpected error occurred while processing {file_name}: {e}")

        return xml_data_list
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def load_data_to_csv(target_file: Path, df: pd.DataFrame):
    df.to_csv(target_file, index=False)

def main():
    """Main entry point for the application."""
    setup_logging()
    logging.info("Application started.")
    logging.info("Downloading XML files from Google Drive...")
    nfe_xml_list = download_nfe_xml_from_drive(NFE_DRIVE_FOLDER_ID, CREDENTIALS_FILE)
    logging.info("XML files downloaded.")
    logging.info("Extracting root from local XML file...")
    infNFe = extract_from_nfe(xml_path)
    logging.info("Root extracted.")
    logging.info("Application finished.")
    print("root from gdrive")
    print("xml content: ")
    print(nfe_xml_list, "hello")
    print()
    for _ in nfe_xml_list:
            print(_,"hoha")
    print("root from local")
    print("xml path: ", xml_path)
    print(infNFe)
    for _ in infNFe:
        print(_)

if __name__ == "__main__":
    main()