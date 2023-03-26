#datetime
from datetime import timedelta, datetime

# Imports
import airflow
import json
import spotipy
import pandas as pd
import numpy as np
import psycopg2
import pickle
import boto3

from io import BytesIO

from sklearn.cluster import KMeans

from spotipy.oauth2 import SpotifyClientCredentials

# Operators
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
 'owner': 'airflow',
}

with DAG (
 'MLWeeklyPipeline',
 default_args=default_args,
 description='RecDaily',
 schedule_interval= '@daily',
 start_date=datetime(2023, 2, 2),
 catchup=False,
) as dag:

    def pull_user_data(**kwargs):
        ti = kwargs['ti']
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT username, email, spotify_url FROM user_data;")
        db_data = cursor.fetchall()
        conn.close()
        ti.xcom_push('user_data', db_data)

    def retrieveMLModel(**kwargs):
        ti = kwargs['ti']
        s3 = boto3.client('s3')
        buffer = BytesIO()

        s3.download_fileobj('is3107', 'model', buffer)

        pkl_data = buffer.getvalue()
        model = pickle.load(pkl_data)

        ti.xcom_push('model', model)


    def pull_spotify_historyAndPredict(**kwargs):
        ti = kwargs['ti']
        db_data = ti.xcom_pull(task_ids = 'pull_user_data', key = 'user_data')
        model = ti.xcom_pull(task_ids = 'retrieveMLModel', key = 'model')

        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        credentials = json.load(open('../Spotify_Scrape/authorization.json'))
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        for (username, email, playlist_id) in db_data:
            results = sp.user_playlist(username, playlist_id, 'tracks')
            playlist_tracks_data = results['tracks']
            ##### Need to add based on how we want to do our model




            ####
            recommendation = json.dumps(model.predict(playlist_tracks_data))

            cursor.execute("UPDATE user_data SET recommendations = ? WHERE email = ? AND username = ?", (recommendation, email, username))
        conn.close()
  
    pull_user_data = PythonOperator(
        task_id = 'pull_user_data',
        python_callable = pull_user_data
    )

    retrieveMLModel = PythonOperator(
        task_id = 'retrieveMLModel',
        python_callable = retrieveMLModel
    )

    pull_spotify_historyAndPredict = PythonOperator(
        task_id = 'pull_spotify_historyAndPredict',
        python_callable = pull_spotify_historyAndPredict
    )

    pull_user_data >> retrieveMLModel >> pull_spotify_historyAndPredict