from src.logger import logger
from src.pipelines.data_ingestion import DataIngestion
from src.pipelines.raw_data_storage import RawDataStorage


#1. Data Ingestion
logger.info("Data Ingestion Started")
ingestion=DataIngestion()
try:
    kaggle_df,gdrive_df=ingestion.run()
    logger.info("Data Ingestion was successfull")
except Exception as e:
    raise e
#2. Storing Raw Data
raw_data_storage=RawDataStorage(kaggle_df=kaggle_df,gdrive_df=gdrive_df)
try:
    raw_data_storage.run_data_storage()
    logger.info("Raw data was stored successfully!")
except Exception as e:
    raise e