import os
import sys
from dataclasses import dataclass
import pandas as pd
from sklearn.model_selection import train_test_split
import mysql.connector
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

from src.logger import logging
from src.exception import CustomException


@dataclass
class DataIngestionConfig:
    train_data_path: str = os.path.join('artifacts', 'train.csv')
    test_data_path: str = os.path.join('artifacts', 'test.csv')
    raw_data_path: str = os.path.join('artifacts', 'raw.csv')


class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info('Data Ingestion method starts')
        conn = None
        try:
            # ---------------------------
            # 1. Connect to MySQL database
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="Admin@123",
                database="Backorder"
            )
            logging.info('Connected to MySQL database')

            # ---------------------------
            # 2. Read table into pandas dataframe
            query = "SELECT * FROM backorder_data"
            df = pd.read_sql(query, conn)
            logging.info('Data fetched from backorder_data table')

            # ---------------------------
            # 3. Convert Yes/No columns to 1/0 (if any)
            yes_no_cols = [
                'potential_issue', 'deck_risk', 'oe_constraint',
                'ppap_risk', 'stop_auto_buy', 'rev_stop', 'went_on_backorder'
            ]
            for col in yes_no_cols:
                if col in df.columns:
                    df[col] = df[col].map({'Yes': 1, 'No': 0})
            logging.info('Converted Yes/No columns to integers')

            # ---------------------------
            # 4. Save raw data locally
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)
            df.to_csv(self.ingestion_config.raw_data_path, index=False)
            logging.info(f'Raw data saved locally at {self.ingestion_config.raw_data_path}')

            # ---------------------------
            # 5. Train-test split
            logging.info('Performing train-test split')
            train_set, test_set = train_test_split(df, test_size=0.30, random_state=42)

            # # Save CSVs locally
            # train_set.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            # test_set.to_csv(self.ingestion_config.test_data_path, index=False, header=True)
            logging.info('Train and test data saved locally as CSVs')

            # ---------------------------
            # 6. Save train and test datasets back to MySQL
            url = URL.create(
                drivername="mysql+mysqlconnector",
                username="root",
                password="Admin@123",
                host="localhost",
                database="Backorder"
            )
            engine = create_engine(url)

            train_set.to_sql(name='backorder_train', con=engine, if_exists='replace', index=False)
            test_set.to_sql(name='backorder_test', con=engine, if_exists='replace', index=False)
            logging.info('Train and test data saved in MySQL database as backorder_train and backorder_test')

            logging.info('Data Ingestion Completed')
            return self.ingestion_config.train_data_path, self.ingestion_config.test_data_path

        except Exception as e:
            logging.error('Exception occurred at Data Ingestion stage')
            raise CustomException(e, sys)
        finally:
            if conn is not None and conn.is_connected():
                conn.close()
                logging.info('MySQL connection closed')
