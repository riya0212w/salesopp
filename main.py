from azure.storage.blob.aio import BlobServiceClient  # Async version of BlobServiceClient
import pandas as pd
import io
import re
from fastapi import FastAPI, HTTPException
import os
import logging
 
# Load environment variables
 
# FastAPI setup
app = FastAPI()
 
# Get connection string from environment variables
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
 
# Azure Blob Storage setup
async def get_blob_service_client():
    return BlobServiceClient.from_connection_string(connection_string)
 
# Sanitize sheet names to avoid invalid characters in Excel
def sanitize_sheet_name(name):
    return re.sub(r'[\\/*:?[\]]', '_', str(name))
 
# Function to create Excel file with separate sheets
def create_excel_with_sheets(dataframe, column_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Add a sheet for each unique value in the specified column
        for value in dataframe[column_name].unique():
            sheet_data = dataframe[dataframe[column_name] == value]
            sanitized_sheet_name = sanitize_sheet_name(value)
            sheet_data.to_excel(writer, sheet_name=sanitized_sheet_name, index=False)
       
        # Add a sheet that contains all records from the CSV file
        all_records_sheet_name = "Total"
        dataframe.to_excel(writer, sheet_name=all_records_sheet_name, index=False)
   
    output.seek(0)
    return output
 
# Process CSV files and create output Excel file
async def process_csv_files_to_excel():
    # Hardcoded values for container name, folder name, etc.
    container_name = 'riyatest'
    input_folder_name = '2024-November'  # Folder where CSV files are stored
    output_folder_name = '2024-November-Processed'  # Folder where output Excel files will be saved
 
    blob_service_client = await get_blob_service_client()
 
    # Access the container
    container_client = blob_service_client.get_container_client(container_name)
 
    # List all blobs in the input folder
    blob_list = container_client.list_blobs(name_starts_with=f'{input_folder_name}/')
 
    # Process each CSV file
    async for blob in blob_list:
        if blob.name.endswith('.csv'):
            sanitized_filename = blob.name.split('/')[-1].replace(".csv", "")
 
            blob_client = container_client.get_blob_client(blob=blob.name)
 
            # Download CSV data into a pandas DataFrame
            csv_data = await blob_client.download_blob()
            csv_data = await csv_data.readall()
            df = pd.read_csv(io.BytesIO(csv_data))
 
            # Modify the CSV and create an Excel file with multiple sheets
            excel_data = create_excel_with_sheets(df, column_name='Last_Transaction_Date_Range')
 
            # Define the output Excel file path
            output_blob_name = f"{output_folder_name}/{sanitized_filename}.xlsx"
            output_blob_client = container_client.get_blob_client(blob=output_blob_name)
 
            # Upload the modified Excel file to Blob Storage
            await output_blob_client.upload_blob(excel_data, overwrite=True)
            logging.info(f"Excel file '{output_blob_name}' created and uploaded.")
 
# FastAPI endpoint to trigger processing of CSV files
@app.post("/process_csv_to_excel/")
async def process_csv():
    try:
        await process_csv_files_to_excel()
        return {"message": "Processing started successfully."}
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error occurred: {str(e)}")
