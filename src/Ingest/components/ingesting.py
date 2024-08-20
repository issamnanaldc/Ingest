import requests
from Ingest import logger
from Ingest.entity.config_entity import IngestConfig

from Ingest.constants import *
from Ingest.utils.common import read_yaml, get_microsoft_data, sanitize_column_name, send_dataframe_to_azure_table

import dotenv
import logging
# Load environment variables
dotenv.load_dotenv()

class Ingest:
    def __init__(self, config: IngestConfig):
        self.config = config
    

    def ingest_data(self):
        microsoft_data = get_microsoft_data(period="5d")
        send_dataframe_to_azure_table(microsoft_data, self.config.connection_string, self.config.table_name)


