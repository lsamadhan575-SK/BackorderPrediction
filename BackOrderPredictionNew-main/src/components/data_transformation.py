import sys
from dataclasses import dataclass

import numpy as np 
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder,StandardScaler

from src.exception import CustomException
from src.logger import logging
import os
from src.utils import save_object

import mysql.connector
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

@dataclass
class DataTransformationConfig:
    preprocessor_obj_file_path=os.path.join('artifacts','preprocessor.pkl')
    preprocessor1_obj_file_path=os.path.join('artifacts','preprocessor1.pkl')
    
    

class DataTransformation:
    def __init__(self):
        self.data_transformation_config=DataTransformationConfig()

    def get_data_transformation_object(self):
        try:
            logging.info('Data Transformation initiated')
            # Define which columns should be ordinal-encoded and which should be scaled
            categorical_columns =[
                'potential_issue',
                'deck_risk',
                'oe_constraint',
                'ppap_risk',
                'stop_auto_buy',
                'rev_stop'
                ]
            
            target_colums =['went_on_backorder']


            numerical_columns = [
                'national_inv',
                'lead_time',
                'in_transit_qty',
                'forecast_3_month',
                'forecast_6_month',
                'forecast_9_month',
                'sales_1_month',
                'sales_3_month',
                'sales_6_month',
                'sales_9_month',
                'min_bank',
                'pieces_past_due',
                'perf_6_month_avg',
                'perf_12_month_avg',
                'local_bo_qty']
            
            # Define the custom ranking for each ordinal variable
            potential_issue_categories=['No', 'Yes']
            deck_risk_categories=['No', 'Yes']
            oe_constraint_categories=['No', 'Yes']
            ppap_risk_categories=['No', 'Yes']
            stop_auto_buy_categories=['No', 'Yes']
            rev_stop_categories=['No', 'Yes']
            went_on_backorder_categories=['No', 'Yes']
            
            logging.info('Pipeline Initiated')

            ## Numerical Pipeline-----------------------('scaler',StandardScaler())
            num_pipeline=Pipeline(
                steps=[
                ('imputer',SimpleImputer(strategy='median'))
                
                ]
            )

            # Categorigal Pipeline-----------------('scaler',StandardScaler())
            cat_pipeline=Pipeline(
                steps=[
                ('imputer',SimpleImputer(strategy='most_frequent')),
                ('ordinalencoder',OrdinalEncoder(categories=[potential_issue_categories,deck_risk_categories,oe_constraint_categories,ppap_risk_categories,stop_auto_buy_categories,rev_stop_categories],handle_unknown='use_encoded_value',unknown_value=-1 ))
                
                ]

            )

            target_pipeline=Pipeline(
                steps=[
                    ('imputer',SimpleImputer(strategy='most_frequent')),
                    ('label_encoder' ,OrdinalEncoder(categories=[went_on_backorder_categories],handle_unknown='use_encoded_value',unknown_value=-1))
            ])

            preprocessor=ColumnTransformer([
            
            ('num_pipeline',num_pipeline,numerical_columns),
            ('cat_pipeline',cat_pipeline,categorical_columns),
            ('out_pipeline',target_pipeline,target_colums)]
            )

#--------------------------------------------------------------------------------------------------------------------------------------
 
            preprocessor1=ColumnTransformer([
            
            ('num_pipeline',num_pipeline,numerical_columns),
            ('cat_pipeline',cat_pipeline,categorical_columns)]
            )

            
            return preprocessor , preprocessor1

            logging.info('Pipeline Completed')

        except Exception as e:
            logging.info("Error in Data Trnasformation")
            raise CustomException(e,sys)
        
    # def initaite_data_transformation(self,train_path,test_path):
    #     try:
    #         # Reading train and test data
    #         train_df = pd.read_csv(train_path)
    #         test_df = pd.read_csv(test_path)

    #         logging.info('Read train and test data completed')
    #         logging.info(f'Train Dataframe Head : \n{train_df.head().to_string()}')
    #         logging.info(f'Test Dataframe Head  : \n{test_df.head().to_string()}')

    #         logging.info('Obtaining preprocessing object')

    #         preprocessing_obj,preprocessor1_obj= self.get_data_transformation_object()

    #         #target_column_name = 'went_on_backorder'
    #         drop_columns = ['sku']  #"Unnamed: 0",

    #         input_feature_train_df = train_df.drop(columns=drop_columns,axis=1)
    #         #target_feature_train_df=train_df[target_column_name]

    #         input_feature_test_df=test_df.drop(columns=drop_columns,axis=1)
    #         #target_feature_test_df=test_df[target_column_name]
            
    #         ## Trnasformating using preprocessor obj
    #         input_feature_train_arr=preprocessing_obj.fit_transform(input_feature_train_df)
            
    #         input_feature_test_arr=preprocessing_obj.transform(input_feature_test_df)

    #         #-------------------------------------------------------------------
    #         zand=preprocessor1_obj.fit_transform(input_feature_train_df)



    #         #logging.info(f'Train Dataframe Head : \n{input_feature_train_arr.to_string()}')
    #         #logging.info(f'Test Dataframe Head  : \n{input_feature_test_arr.to_string()}')

    #         logging.info("Applying preprocessing object on training and testing datasets.")
            
    #         train_arr = np.c_[input_feature_train_arr]
    #         test_arr = np.c_[input_feature_test_arr]

    #         save_object(

    #             file_path=self.data_transformation_config.preprocessor_obj_file_path,
    #             obj=preprocessing_obj

    #         )

    #         save_object(

    #             file_path=self.data_transformation_config.preprocessor1_obj_file_path,
    #             obj=preprocessor1_obj

    #         )
    #         logging.info('Preprocessor pickle file saved')

    #         return (
    #             train_arr,
    #             test_arr,
    #             self.data_transformation_config.preprocessor_obj_file_path,
    #         )
            
    #     except Exception as e:
    #         logging.info("Exception occured in the initiate_datatransformation")

    #         raise CustomException(e,sys)

    def initaite_data_transformation(self):
        try:
            logging.info('Reading train and test data from MySQL database')

            # ---------------------------
            # Connect to MySQL database
            url = URL.create(
                drivername="mysql+mysqlconnector",
                username="root",
                password="Admin@123",  # replace with your password
                host="localhost",
                database="Backorder"
            )
            engine = create_engine(url)

            # Read train and test tables
            train_df = pd.read_sql("SELECT * FROM backorder_train", engine)
            test_df = pd.read_sql("SELECT * FROM backorder_test", engine)

            logging.info('Read train and test data completed from DB')
            logging.info(f'Train Dataframe Head : \n{train_df.head().to_string()}')
            logging.info(f'Test Dataframe Head  : \n{test_df.head().to_string()}')

            logging.info('Obtaining preprocessing object')
            preprocessing_obj, preprocessor1_obj = self.get_data_transformation_object()

            drop_columns = ['sku']  # "Unnamed: 0" can also be dropped if needed

            input_feature_train_df = train_df.drop(columns=drop_columns, axis=1)
            input_feature_test_df = test_df.drop(columns=drop_columns, axis=1)
            print("Input feature train df shape:")
            print(input_feature_train_df.head())
            print(input_feature_test_df.head())
            # ---------------------------
            # Transform using preprocessing objects
            input_feature_train_arr = preprocessing_obj.fit_transform(input_feature_train_df)
            input_feature_test_arr = preprocessing_obj.transform(input_feature_test_df)

            # Optionally apply second preprocessor
            zand = preprocessor1_obj.fit_transform(input_feature_train_df)

            logging.info("Applying preprocessing object on training and testing datasets.")
            # train_valid_idx = input_feature_train_arr[:, -1] != -1
            # test_valid_idx = input_feature_test_arr[:, -1] != -1

            # train_arr = input_feature_train_arr[train_valid_idx]
            # test_arr = input_feature_test_arr[test_valid_idx]
            train_arr = np.c_[input_feature_train_arr]
            test_arr = np.c_[input_feature_test_arr]
            print("Transformed training array shape:")
            print(train_arr.shape)  
            print("Transformed testing array shape:")
            print(test_arr.shape)
            # Save preprocessing objects
            save_object(file_path=self.data_transformation_config.preprocessor_obj_file_path,
                        obj=preprocessing_obj)

            save_object(file_path=self.data_transformation_config.preprocessor1_obj_file_path,
                        obj=preprocessor1_obj)

            logging.info('Preprocessor pickle files saved')

            return train_arr, test_arr, self.data_transformation_config.preprocessor_obj_file_path

        except Exception as e:
            logging.error("Exception occurred in the initiate_data_transformation")
            raise CustomException(e, sys)
