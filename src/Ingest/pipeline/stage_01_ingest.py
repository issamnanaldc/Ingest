from Ingest.config.configuration import ConfigurationManager
from Ingest.components.ingesting import Ingest
from Ingest import logger


STAGE_NAME = "Ingest changes"


class IngestPipeline:
    def __init__(self) -> None:
        pass
    
    def main(self):
        # for _ in range(5):
            
            config = ConfigurationManager()
            ingest_config = config.get_ingestion_config()
            ingesting = Ingest(config=ingest_config)
            ingest_data = ingesting.ingest_data()



if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        obj = IngestPipeline()
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<")
    except Exception as e:
        logger.exception(e)
        raise e