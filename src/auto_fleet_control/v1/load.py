import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
from dotenv import load_dotenv
import os

load_dotenv

# Path to your Google Sheets API credentials JSON file
# credentials_path = os.getenv("google_sheet_credentials_file")
credentials_path = "key-file.json"

# Define the scope for Google Sheets API
SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

def load_data_to_google_sheets(spreadsheet_key, transformed_data):
    """Upload transformed data to Google Sheets."""
    try:
        # Authenticate with Google Sheets API
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
        gc = gspread.authorize(creds)

        # Extract the spreadsheet key from the URL
        # spreadsheet_key = sheet_url.split('/d/')[1].split('/')[0]

        # Open the Google Sheet by its key
        spreadsheet = gc.open_by_key(spreadsheet_key)

        # Select the first worksheet (you might need to specify a different one)
        worksheet = spreadsheet.worksheet('dados')

        # Clear existing data in the worksheet
        worksheet.clear()

        # Upload the DataFrame to the worksheet, including headers
        worksheet.update([transformed_data.columns.values.tolist()] + transformed_data.values.tolist())

        logging.info(f"Data successfully uploaded to Google Sheet: {spreadsheet.title}")

    except Exception as e:
        logging.error(f"An error occurred during data loading to Google Sheets: {e}")
