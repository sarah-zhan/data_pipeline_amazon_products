#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import re
from google.cloud import storage
import os

# set the environment variable for the service account file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/labber/data-engineering.json"

df = pd.read_csv("amazon_products.csv")
df_category = pd.read_csv('amazon_categories.csv')
df_columns = df.columns

# standardize the column names of df
for col in df_columns:
    converted_col = re.sub(r'(?<=[a-z])([A-Z])', r'_\1', col).lower()
    df.rename(columns={col: converted_col}, inplace=True)

# merge df and df_category
import pandas as pd
merge_df = pd.merge(df, df_category, left_on='category_id', right_on='id', how='left')
merge_df.drop('id', axis=1, inplace=True)

# save the merged dataframe to a csv file
merge_df.to_csv('merge_df.csv', index=False)

# upload the merged csv file to GCS bucket
def upload_to_gcs(bucket_name, file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_name)
    print(f'File {file_name} uploaded to {destination_blob_name}.')

# set the bucket name and file name
bucket_name = 'datapipline_amazon_products'
file_name = 'merge_df.csv'
destination_blob_name = 'merge_df.csv'

# upload the file to GCS
upload_to_gcs(bucket_name, file_name, destination_blob_name)