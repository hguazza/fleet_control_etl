import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "pdf_transformed_data.csv"
pdf_path = "C:\\Users\\Henrique\\Dev\\Python\\auto_fleet_control\\notas_pdf\\test.pdf"

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def extract_from_pdf(pdf_path):
    """Extract data from PDF file."""
    # Read PDF file using tabula
    df = read_pdf(pdf_path, pages='all', multiple_tables=True)
    
    # Concatenate all tables into a single DataFrame
    # combined_df = pd.concat(df, ignore_index=True)
    
    # # Clean up the DataFrame (if necessary)
    # combined_df.columns = [col.strip() for col in combined_df.columns]
    
    print(df)

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def main():
    """Main entry point for the application."""
    setup_logging()
    # logging.info("Application started.")
    # logging.info("Extracting data from PDF...")
    extract_from_pdf(pdf_path)
    # logging.info("Data extraction complete.")
    # logging.info("Loading data to CSV file...")
    # # load_data(target_file, extract_from_pdf(pdf_path))
    # logging.info("Application finished.")

if __name__ == "__main__":
    main()