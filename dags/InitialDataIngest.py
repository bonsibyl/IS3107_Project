#datetime
from datetime import timedelta, datetime

# Imports
import airflow
import json
import spotipy
import pandas as pd
import numpy as np
import psycopg2
import boto3
import os

from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

from spotipy.oauth2 import SpotifyClientCredentials

# Operators
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
 'owner': 'airflow',
}

with DAG (
 'InitialDataIngestPipeline',
 default_args=default_args,
 description='InitialDataIngestWeekly',
 schedule_interval= None,
 start_date=datetime(2023, 2, 2),
 catchup=False,
) as dag:

    def createTables(**kwargs):
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432'
        )
  
        conn.autocommit = True
        cursor = conn.cursor()

        create_table_user = '''CREATE TABLE IF NOT EXISTS user_data (
            username varchar(30) PRIMARY KEY,
            playlist_id varchar(200),
            email varchar(100) UNIQUE NOT NULL,
            password varchar(50) NOT NULL
        );
        '''

        create_table_rec = '''CREATE TABLE IF NOT EXISTS recommendation_data (
            username varchar(30) PRIMARY KEY REFERENCES user_data(username) ON DELETE CASCADE,
            recommendation text[], rec_explanation text[], rec_links text[]
        );
        '''

        create_table_viz = '''CREATE TABLE IF NOT EXISTS visualisation_data (
            username varchar(30) PRIMARY KEY REFERENCES user_data(username) ON DELETE CASCADE,
            danceability numeric,
            energy numeric,
            key numeric,
            loudness numeric,
            acousticness numeric,
            instrumentalness numeric,
            liveness numeric,
            valence numeric,
            tempo numeric,
            most_freq_artists jsonb,
            genres jsonb
        );
        '''

        create_table_training = '''CREATE TABLE IF NOT EXISTS training_data (
            valence numeric,
            year int,
            acousticness numeric,
            artists jsonb,
            danceability numeric,
            duration_ms numeric,
            energy numeric,
            id varchar(50) PRIMARY KEY,
            instrumentalness numeric,
            key int,
            liveness numeric,
            loudness numeric,
            mode int,
            name varchar(300),
            tempo numeric
        );
        '''
        cursor.execute(create_table_user)
        cursor.execute(create_table_rec)
        cursor.execute(create_table_viz)
        cursor.execute(create_table_training)
        conn.close()


    def transformSongData(**kwargs):
        new_file = pd.read_csv('airflow/data/data.csv')
        new_file = new_file.drop(columns=['explicit', 'popularity', 'release_date', 'speechiness'])
        new_file['artists'] = [json.dumps(artists) for artists in new_file['artists']]
        df_numeric = new_file.select_dtypes(include=['int', 'float'])

        # Create a MinMaxScaler object
        scaler = MinMaxScaler()
        # Fit and transform the DataFrame
        df_scaled = scaler.fit_transform(df_numeric)
        # Create a new Pandas DataFrame with the scaled data
        df_scaled = pd.DataFrame(df_scaled, columns=df_numeric.columns)

        # Combine the scaled numerical columns with the non-numerical columns
        df_scaled = pd.concat([df_scaled, new_file.select_dtypes(exclude=['int', 'float'])], axis=1)
        df_scaled['year'] = new_file['year']
        df_scaled['key'] = new_file['key']
        df_scaled['mode'] = new_file['mode']
        df_scaled = df_scaled[new_file.columns]
        df_scaled.to_csv('airflow/data/song_data.csv', index = False)

    def ingestSongData(**kwargs):
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        file = open('airflow/data/song_data.csv', 'r', encoding='utf-8')
        try:
            ingest_data = '''COPY training_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
            cursor.copy_expert(ingest_data, file)
        except:
            print("Files already exist!")
        conn.close()


    def ingestPlaylistData(**kwargs):
        s3 = boto3.client('s3', aws_access_key_id="AKIAY73PMJYUU6QFJTOP", aws_secret_access_key="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")

        s3_directory = 'is3107-spotify'

        # Define the local directory where the JSON files are stored
        print(os.getcwd())
        local_directory = 'airflow/data'

        # Iterate through all files in the local directory
        for file_name in os.listdir(local_directory):
            # Check if the file is a JSON file
            if file_name.endswith('.json'):
                # Define the local file path
                local_file_path = os.path.join(local_directory, file_name)
                # Upload the file to S3
                s3.upload_file(local_file_path, s3_directory, file_name)

    createTables = PythonOperator(
        task_id = 'createTables',
        python_callable = createTables
    )

    transformSongData = PythonOperator(
        task_id = 'transformSongData',
        python_callable = transformSongData
    )

    ingestSongData = PythonOperator(
        task_id = 'ingestSongData',
        python_callable = ingestSongData
    )

    ingestPlaylistData = PythonOperator(
        task_id = 'ingestPlaylistData',
        python_callable = ingestPlaylistData
    )

    createTables >> transformSongData >> ingestSongData
    createTables >> ingestPlaylistData