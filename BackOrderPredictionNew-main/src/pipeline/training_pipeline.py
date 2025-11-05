import os
import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd

from src.components.data_ingestion_copy import DataIngestion
from src.components.data_transformation import DataTransformation
from src.components.model_trainer import ModelTrainer


if __name__=='__main__':
    obj=DataIngestion()
    # train_data_path,test_data_path=obj.initiate_data_ingestion()
    print("------------------Data Ingestion Completed---------------------")
    data_transformation = DataTransformation()
    # train_arr,test_arr,_=data_transformation.initaite_data_transformation(train_data_path,test_data_path)
    train_arr,test_arr,_=data_transformation.initaite_data_transformation()
    print("------------------Data Transformation Completed---------------------")
    model_trainer=ModelTrainer()
    print("------------------Model Training Started---------------------")  
    print(train_arr[1:5])
    print(test_arr.shape)
    model_trainer.initate_model_training(train_arr,test_arr)