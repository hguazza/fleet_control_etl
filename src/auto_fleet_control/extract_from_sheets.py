import pandas as pd

def extract_from_google_sheets(sheet_url):
    """Read a Google Sheet and convert it to a DataFrame."""

    # The part of the URL that contains the sheet ID
    sheet_id = sheet_url.split("/d/")[1].split("/edit")[0]
    gid = sheet_url.split("gid=")[1] if "gid=" in sheet_url else "0"
    
    # Construct the CSV export URL
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    try:
        df = pd.read_csv(csv_url)
        print("DataFrame created successfully:")
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure the Google Sheet is publicly accessible.")
    return df