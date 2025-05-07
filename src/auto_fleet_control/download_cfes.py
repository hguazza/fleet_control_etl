from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import io
import xml.etree.ElementTree as ET

def download_cfe_xml_from_drive(folder_id: str, credentials_file: str) -> list:
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

        print(f"Found {len(items)} XML files in the folder.")
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
                xml_data_list.append(root)
                print(f"Successfully downloaded and parsed: {file_name}")

            except HttpError as error:
                print(f"An error occurred while downloading or parsing {file_name}: {error}")
            except ET.ParseError as error:
                print(f"Error parsing XML in {file_name}: {error}")

        return xml_data_list

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    

def debug_list_files(folder_id: str, credentials_file: str):
    """Lists all files and their MIME types in the specified Google Drive folder."""
    try:
        creds = Credentials.from_service_account_file(credentials_file,
                                                     scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(name, mimeType)").execute()
        items = results.get('files', [])
        if not items:
            print(f"No files found in folder with ID: {folder_id}")
            return
        print(f"Files in folder '{folder_id}':")
        for item in items:
            print(f"- Name: {item['name']}, MIME Type: {item.get('mimeType')}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    # Replace with your actual folder ID and credentials file path
    YOUR_DRIVE_FOLDER_ID = '1v74MOjBS7bnzWO9gsIaOqOBrL1zm4JEU'
    YOUR_CREDENTIALS_FILE = 'key-file.json'

    xml_roots = download_cfe_xml_from_drive(YOUR_DRIVE_FOLDER_ID, YOUR_CREDENTIALS_FILE)

    if xml_roots:
        print("\nSuccessfully downloaded and parsed all XML files.")
        print(f"Number of XML files processed: {len(xml_roots)}")
        # You can now work with the list of XML root elements
        # For example, print the tag of the root element of the first XML file:
        if xml_roots:
            print(xml_roots)
            print("\nExample: Root tag of the first XML file:", xml_roots[0].tag)
    else:
        print("\nNo XML data was downloaded.")
    # debug_list_files(YOUR_DRIVE_FOLDER_ID, YOUR_CREDENTIALS_FILE)    