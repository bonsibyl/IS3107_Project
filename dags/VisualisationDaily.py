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

        cursor.execute("SELECT username, email, playlist_id FROM user_data;")
        db_data = cursor.fetchall()
        conn.close()
        ti.xcom_push('user_data', db_data)

    def calculatePlaylistStats(**kwargs):
        ti = kwargs['ti']
        db_data = ti.xcom_pull(task_ids = 'pull_user_data', key = 'user_data')
        
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
            playlist_tracks_id = []
            playlist_tracks_first_artists = []

            for track in playlist_tracks_data['items']:
                playlist_tracks_id.append(track['track']['id'])
                playlist_tracks_first_artists.append(track['track']['artists'][0])
            features = sp.audio_features(playlist_tracks_id)
            features_df = pd.DataFrame(data=features, columns=features[0].keys())

            features_df['first_artist'] = playlist_tracks_first_artists
            features_df = features_df[['id', 'first_artist',
                                    'danceability', 'energy', 'key', 'loudness',
                                    'mode', 'acousticness', 'instrumentalness',
                                    'liveness', 'valence', 'tempo',
                                    'duration_ms', 'time_signature']]
            numerical_cols = features_df.select_dtypes(include='number')
            mean_row = numerical_cols.mean()
            mean_row['most_freq_artist'] = json.dumps(features_df['first_artist'].value_counts().nlargest(5).index.tolist())
            cursor.execute('''UPDATE visualisation_data 
                              SET danceability = ?, energy = ?, key = ?, loudness = ?, acousticness = ?, instrumentalness = ?, liveness = ?, valence = ?, tempo = ?, most_freq_artist = ?
                              WHERE username = ?''', (mean_row.iloc[0]['danceability'], mean_row.iloc[0]['energy'], mean_row.iloc[0]['key'], mean_row.iloc[0]['loudness'],
                              mean_row.iloc[0]['acousticness'], mean_row.iloc[0]['instrumentalness'], mean_row.iloc[0]['liveness'], mean_row.iloc[0]['valence'], 
                              mean_row.iloc[0]['tempo'], mean_row.iloc[0]['most_freq_artist'], username)
                            )
        conn.close()
  
    pull_user_data = PythonOperator(
        task_id = 'pull_user_data',
        python_callable = pull_user_data
    )

    calculatePlaylistStats = PythonOperator(
        task_id = 'calculatePlaylistStats',
        python_callable = calculatePlaylistStats
    )

    pull_user_data >> calculatePlaylistStats