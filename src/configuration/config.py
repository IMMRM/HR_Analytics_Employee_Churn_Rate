from src.constants import CONFIG_PATH,PARAMS_PATH,SCHEMA_PATH
from src.utils.common import read_yaml
from src.entity.data_ingestion import DataIngestionConfig




class ConfigurationManager:
    def __init__(self,config_path=CONFIG_PATH,params_path=PARAMS_PATH,schema_path=SCHEMA_PATH):
        self.config=read_yaml(config_path),
        self.params=read_yaml(params_path),
        self.schema=read_yaml(schema_path)
    def get_data_ingestion_config(self)->DataIngestionConfig:
        config=self.config[0].data
        data_ingestion=DataIngestionConfig(
            kaggle_dataset=config.kaggle_source_path,
            gdrive_dataset=config.gdrive_source_path,
            local_dataset=config.raw,
            staging_dataset=config.interim
        )
        return data_ingestion