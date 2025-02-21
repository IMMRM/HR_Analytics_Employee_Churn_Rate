import os
import pandas as pd
import kagglehub
import shutil
import gdown
from src.configuration.config import ConfigurationManager
from src.logger import logger

class DataIngestion:
    def __init__(self):
        self.config=ConfigurationManager().get_data_ingestion_config()
    def download_kaggle_data(self):
        logger.info("Starting to download the kaggle dataset")
        path = kagglehub.dataset_download(self.config.kaggle_dataset,force_download=True)
        if(os.path.exists(self.config.local_dataset+"1/") and os.path.isdir(self.config.local_dataset+"1/")):
            shutil.rmtree(self.config.local_dataset+"1/")
        shutil.move(path,self.config.local_dataset)
        logger.info("Kaggle dataset download completed!")
        return pd.read_excel(self.config.local_dataset+'1/E Commerce Dataset.xlsx',sheet_name=1)
        
    def gdrive_data(self):
        #dowload data from public dataset hosted in gdrive
        if(os.path.exists(self.config.local_dataset+"EComm_gdrive_raw_data.xlsx")):
            os.remove(self.config.local_dataset+"EComm_gdrive_raw_data.xlsx")
        logger.info("Staring to download Google Drive Data")
        gdown.download(self.config.gdrive_dataset,"data/raw/EComm_gdrive_raw_data.xlsx",quiet=False)
        logger.info("Google Drive data download completed!")
        return pd.read_excel(self.config.local_dataset+'EComm_gdrive_raw_data.xlsx',sheet_name=1,engine='openpyxl')
    def run(self):
        logger.info("Beggining to combine the dataset")
        kaggle_data_df=self.download_kaggle_data()
        gdrive_data_df=self.gdrive_data()
        
        #Ensuring both datasets have same schema
        total_rows=len(kaggle_data_df)
        half_size=total_rows//2
        
        half_kaggle=kaggle_data_df.iloc[:half_size]
        half_gdrive=gdrive_data_df.iloc[-half_size:]
        
        combined_data=pd.concat([half_kaggle,half_gdrive],ignore_index=True)
        logger.info("Dataset have been combined and moved to staging area under interim folder")
        return combined_data.to_csv(self.config.staging_dataset+"Combined_dataset_churn_prediction.csv")
    
        
        
        
            
        
        
        
