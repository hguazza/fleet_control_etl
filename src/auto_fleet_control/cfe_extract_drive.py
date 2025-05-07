from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import io
import xml.etree.ElementTree as ET
import googleapiclient
import re
import pandas as pd
from datetime import datetime

# --- Configuration ---
# Replace with the ID of your Google Drive folder
DRIVE_FOLDER_ID = '1v74MOjBS7bnzWO9gsIaOqOBrL1zm4JEU'  

# Path to your service account key JSON file
SERVICE_ACCOUNT_KEY_PATH = 'key-file.json' 

# Define the scopes needed for Google Drive access
SCOPES = ['https://www.googleapis.com/auth/drive.readonly',
          'https://www.googleapis.com/auth/spreadsheets']

today = datetime.now().strftime("%d/%m/%Y")

def get_xml_data_from_drive() -> list:
    """
    Extracts data directly from XML files within a specified Google Drive folder.

    Returns:
        list: A list where each element is the parsed XML content
              (using ElementTree in this example). You can modify the
              parsing logic as needed.
    """
    creds = None
    extracted_data = []
    try:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH, scopes=SCOPES)

        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/xml'",
            fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print(f'No XML files found in the folder with ID: {DRIVE_FOLDER_ID}.')
            return extracted_data

        for item in items:
            file_name = item['name']
            file_id = item['id']
            print(f'Processing file: {file_name} (ID: {file_id})')
            try:
                # Download the file content directly into memory
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = googleapiclient.http.MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    print(f'Download progress for {file_name}: {int(status.progress() * 100)}%.', end='\r')
                print() # New line after download completion
                xml_content = fh.getvalue()

                # --- Your XML parsing logic here ---
                try:
                    tree = ET.parse(xml_content)
                    root = tree.getroot()
                except (ET.ParseError, FileNotFoundError) as e:
                    raise ValueError(f"Failed to parse XML file {xml_content}: {e}")

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
                        try:
                            vProd = float(vProd_text) if vProd_text else None
                        except ValueError:
                            vProd = None

                        rows.append({
                            "Data": today,
                            "Numero": nCFe,
                            "Valor": vProd,
                            "Descricao": xProd,
                            "Placa": placa,
                            "KM": km,
                            "Fornecedor": fornecedor,
                            "Motorista": "",
                            "Histórico": "",
                            "Categoria": ""
                        })

                return pd.DataFrame(rows)

            except HttpError as error:
                print(f'An error occurred while processing {file_name}: {error}')
                continue

        return extracted_data

    except Exception as e:
        print(f"An error occurred during Google Drive interaction: {e}")
        return extracted_data

# --- Example of how to use the function ---
if __name__ == '__main__':
    drive_folder_link = "YOUR_GOOGLE_DRIVE_FOLDER_LINK" # Reminder to get the Folder ID
    # Extract the folder ID (replace with your actual logic or just the ID string)

    xml_data = get_xml_data_from_drive()
    if xml_data:
        print("\nSuccessfully retrieved and (partially) processed XML data from Google Drive.")
        # Now you can iterate through 'xml_data' and extract the specific
        # information you need to update your Google Sheet.
        # For example:
        # for root in xml_data:
        #     # Extract data using root.findall('.//some_tag') etc.
        #     pass
    else:
        print("\nNo XML data retrieved from Google Drive.")