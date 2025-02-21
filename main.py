from src.logger import logger
from src.pipelines.data_ingestion import DataIngestion


#1. Data Ingestion
logger.info("Data Ingestion Started")
ingestion=DataIngestion()
try:
    ingestion.run()
    logger.info("Data Ingestion was successfull")
except Exception as e:
    raise e