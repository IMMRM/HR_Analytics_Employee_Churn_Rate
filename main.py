from src.logger import logger
from src.pipelines.data_ingestion import DataIngestion
from src.pipelines.raw_data_storage import RawDataStorage
from src.pipelines.data_validation import DataValidation
from src.pipelines.data_preparations import DataPreparation

logger.info(">>>>>>>>>>>>>>>>>>>>>>>> Data Ingestion >>>>>>>>>>>>>>>>>>>>>>>>>>")
#1. Data Ingestion
logger.info("Data Ingestion Started")
ingestion=DataIngestion()
try:
    kaggle_df,gdrive_df=ingestion.run()
    logger.info("Data Ingestion was successfull")
except Exception as e:
    raise e

#2. Storing Raw Data
logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>> Raw Data Storage>>>>>>>>>>>>>>>>>>>>>>>>")
raw_data_storage=RawDataStorage(kaggle_df=kaggle_df,gdrive_df=gdrive_df)
try:
    raw_data_storage.run_data_storage()
    logger.info("Raw data was stored successfully!")
except Exception as e:
    raise e

#3. Run Data Validation
logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>> Data Validation >>>>>>>>>>>>>>>>>>>>>>>>")
data_val=DataValidation()
try:
    if(data_val.run_validation()):
        logger.info("Data was validated and report was generated successfully!")
    else:
        logger.error("Data validation failed")
except Exception as e:
    raise e

#4. Run Data Preparations
logger.info(">>>>>>>>>>>>>>>>>>>>>>> Data Preparations>>>>>>>>>>>>>>>>>>>>>>>>>>")
data_prep=DataPreparation()
logger.info("Data Preparations started")
try:
    data_prep.run_data_preparations()
except Exception as e:
    raise e
    
