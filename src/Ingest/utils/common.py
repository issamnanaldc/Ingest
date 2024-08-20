
import os
from box.exceptions import BoxValueError
import yaml
from Ingest import logger
from ensure import ensure_annotations
from box import ConfigBox
from pathlib import Path
from azure.data.tables import TableServiceClient
import uuid
import re
import yfinance as yf
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient

@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """reads yaml file and returns

    Args:
        path_to_yaml (str): path like input

    Raises:
        ValueError: if yaml file is empty
        e: empty file

    Returns:
        ConfigBox: ConfigBox type
    """
    try:
        with open(path_to_yaml) as yaml_file:
            content = yaml.safe_load(yaml_file)
            logger.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError:
        raise ValueError("yaml file is empty")
    except Exception as e:
        raise e
    


@ensure_annotations
def create_directories(path_to_directories: list, verbose=True):
    """create list of directories

    Args:
        path_to_directories (list): list of path of directories
        ignore_log (bool, optional): ignore if multiple dirs is to be created. Defaults to False.
    """
    for path in path_to_directories:
        os.makedirs(path, exist_ok=True)
        if verbose:
            logger.info(f"created directory at: {path}")



@ensure_annotations
def get_microsoft_data(period="1mo"):
    """
    Fetch historical market data for Microsoft (MSFT) for a specified period.

    Parameters:
    period (str): The period of data to retrieve. Example values include:
                  "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
                  Default is "1mo" (one month).

    Returns:
    pd.DataFrame: A DataFrame containing the historical data.
    """
    # Define the ticker symbol for Microsoft
    ticker_symbol = 'MSFT'
    
    # Use yfinance to get the data
    msft = yf.Ticker(ticker_symbol)
    
    # Get the historical market data for the specified period
    data = msft.history(period=period)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    return df



@ensure_annotations
def sanitize_column_name(name: str) -> str:
    # Replace any non-alphanumeric character with an underscore
    return re.sub(r'[^a-zA-Z0-9]', '_', name)

@ensure_annotations
def send_dataframe_to_azure_table(df: pd.DataFrame, connection_string: str, table_name: str):
    # Sanitize the DataFrame column names to conform to Azure Table Storage requirements
    df.columns = [sanitize_column_name(col) for col in df.columns]
    
    # Create a TableServiceClient using the connection string
    table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
    
    # Ensure the table exists (creates it if it doesn't)
    try:
        table_service_client.create_table_if_not_exists(table_name)
    except Exception as e:
        print(f"Error creating table: {e}")
        return

    # Create a TableClient for the specified table
    table_client = table_service_client.get_table_client(table_name=table_name)

    # Iterate through the DataFrame rows and upload each row to the table
    for index, row in df.iterrows():
        entity = row.to_dict()
        entity['PartitionKey'] = 'partition1'  # You can customize this
        entity['RowKey'] = str(uuid.uuid4())   # RowKey must be unique

        table_client.create_entity(entity=entity)

    print(f"DataFrame successfully uploaded to Azure Table Storage: {table_name}")



@ensure_annotations
def read_table_to_dataframe(connection_string, table_name):
    table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = table_service.get_table_client(table_name)
    
    # Query all entities in the table
    entities = table_client.list_entities()
    
    # Convert entities to list of dictionaries
    entity_list = []
    for entity in entities:
        entity_dict = dict(entity)
        entity_list.append(entity_dict)
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(entity_list)
    return df



@ensure_annotations
def upload_dataframe_to_blob(dataframe: pd.DataFrame, container_name: str, blob_name: str, connection_string: str):
    # Convert the DataFrame to an Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False)
    output.seek(0)  # Go to the start of the file

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Create the container if it does not exist
    try:
        container_client.create_container()
    except Exception as e:
        print(f"Container may already exist: {e}")

    # Get the BlobClient
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the Excel file to Azure Blob Storage
    blob_client.upload_blob(output, overwrite=True)
    print(f"File {blob_name} uploaded to container {container_name} successfully.")
