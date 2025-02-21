from dataclasses import dataclass

@dataclass
class DataStorageConnectionConfig:
    server_name: str
    db_name: str
    driver_name: str
    trusted_conn: str
    kaggle_table: str
    gdrive_table: str