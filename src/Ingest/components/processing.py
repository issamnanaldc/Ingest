import requests
from Ingest import logger
from Ingest.entity.config_entity import ProcessConfig

from Ingest.constants import *
from Ingest.utils.common import read_yaml, read_table_to_dataframe, upload_dataframe_to_blob

import dotenv
import logging
# Load environment variables
dotenv.load_dotenv()

class Process:
    def __init__(self, config: ProcessConfig):
        self.config = config
    

    def start_process(self):
        df = read_table_to_dataframe(self.config.connection_string, self.config.table_name)
        upload_dataframe_to_blob(df, self.config.container_name, 'test_aml.csv', self.config.connection_string)