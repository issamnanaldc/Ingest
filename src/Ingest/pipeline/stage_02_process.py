from Ingest.config.configuration import ConfigurationManager
from Ingest.components.processing import Process
from Ingest import logger


STAGE_NAME = "Process dataframe"


class ProcessPipeline:
    def __init__(self) -> None:
        pass
    
    def main(self):
        
        config = ConfigurationManager()
        process_config = config.get_process_config()
        processing = Process(config=process_config)
        process_data = processing.start_process()



if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        obj = ProcessPipeline()
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<")
    except Exception as e:
        logger.exception(e)
        raise e