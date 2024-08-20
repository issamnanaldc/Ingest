from Ingest.constants import *
from Ingest.utils.common import read_yaml, create_directories
from Ingest.entity.config_entity import IngestConfig, ProcessConfig

import os
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.


class ConfigurationManager:
    def __init__(
        self,
        params_filepath = PARAMS_FILE_PATH):

        self.params = read_yaml(params_filepath)


    
    def get_ingestion_config(self) -> IngestConfig:
        config = self.params


        ingest_config = IngestConfig(

            table_name=config.table_name,

            connection_string=os.getenv('CONNECTION_STRING')
            
        )

        return ingest_config
    

    def get_process_config(self) -> ProcessConfig:

        config = self.params


        process_config = ProcessConfig(

            table_name=config.table_name,

            connection_string=os.getenv('CONNECTION_STRING'),
            
            container_name=config.container_name
        )

        return process_config