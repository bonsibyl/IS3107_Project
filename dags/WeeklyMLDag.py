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
 description='MLWeekly',
 schedule_interval= '@weekly',
 start_date=datetime(2023, 2, 2),
 catchup=False,
) as dag:

    def pull_spotify_data(**kwargs):
        ti = kwargs['ti']

        credentials = json.load(open('../Spotify_Scrape/authorization.json'))
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']

        playlist_uri = 'spotify:playlist:37i9dQZEVXbK4gjvS1FjPY' #top50Singapore

        client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)

        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        uri = playlist_uri    # the URI is split by ':' to get the username and playlist ID
        username = uri.split(':')[1]
        playlist_id = uri.split(':')[2]
        results = sp.user_playlist(username, playlist_id, 'tracks')
        playlist_tracks_data = results['tracks']
        ti.xcom_push('playlist_data', playlist_tracks_data)
        ti.xcom_push('spotify_api_client', sp)
        #### Replace above section with scraped data from spotify ####

    def clean_spotify_data(**kwargs):
        ti = kwargs['ti']
        sp = ti.xcom_pull(task_ids = 'pull_spotify_data', key = 'playlist_data')
        results = ti.xcom_pull(task_ids = 'pull_spotify_data', key = 'playlist_data')
        playlist_tracks_data = results['tracks']
        playlist_tracks_id = []
        playlist_tracks_titles = []
        playlist_tracks_artists = []
        playlist_tracks_first_artists = []

        for track in playlist_tracks_data['items']:
            playlist_tracks_id.append(track['track']['id'])
            playlist_tracks_titles.append(track['track']['name'])
            # adds a list of all artists involved in the song to the list of artists for the playlist
            artist_list = []
            for artist in track['track']['artists']:
                artist_list.append(artist['name'])
            playlist_tracks_artists.append(artist_list)
            playlist_tracks_first_artists.append(artist_list[0])
        features = sp.audio_features(playlist_tracks_id)
        features_df = pd.DataFrame(data=features, columns=features[0].keys())

        #### Modify to match database schema
        features_df['title'] = playlist_tracks_titles
        features_df['first_artist'] = playlist_tracks_first_artists
        features_df['all_artists'] = playlist_tracks_artists
        features_df = features_df[['id', 'title', 'first_artist', 'all_artists',
                                'danceability', 'energy', 'key', 'loudness',
                                'mode', 'acousticness', 'instrumentalness',
                                'liveness', 'valence', 'tempo',
                                'duration_ms', 'time_signature']]
        #### Modify to match database schema ####

        ti.xcom_push('training_data', features_df)

    def train_ML_model(**kwargs):
        ti = kwargs['ti']
        # Pull playlist data
        training_data = ti.xcom_pull(task_ids = 'clean_spotify_data', key = 'training_data')

        #DB Connection
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
  
        conn.autocommit = True
        cursor = conn.cursor()

        # Pull db data
        cursor.execute("SELECT * FROM song_data;")
        db_data = cursor.fetchall()
        db_cols = [cols[0] for cols in cursor.description]
        db_df = pd.DataFrame(data = db_data, colums = db_cols)

        # Combine the two
        combined_data = pd.concat([training_data, db_data]).drop_duplicates().reset_index(drop = True)

        # Train model
        kmeans = KMeans(n_clusters=10, n_jobs = 1).fit(combined_data)

        # Pass model to xcom
        ti.xcom_push('model', kmeans)

        # Upload non-dup data to db
        training_data.to_sql('song_data', conn, if_exists = 'append', index = False)
        conn.close()


    def save_ML_model(**kwargs):
        ti = kwargs['ti']
        ### Pull model from xcom
        model = ti.xcom_pull(task_ids = 'train_ML_model', key = 'model')

        # AWS S3 Client
        s3 = boto3.client('s3')
        
        ### push to s3 bucket
        with open('model.pkl', 'wb') as file:
            pickle.dump(model, file)
            
            #Add code to upload to s3 with keys
            try:
                response = s3.upload(file, 'is3107', 'model.pkl')
            except Exception as e:
                print(e)

    pull_spotify_data = PythonOperator(
        task_id = 'pull_spotify_data',
        python_callable = pull_spotify_data
    )

    clean_spotify_data = PythonOperator(
        task_id = 'clean_spotify_data',
        python_callable = clean_spotify_data
    )

    train_ML_model = PythonOperator(
        task_id = 'train_ML_model',
        python_callable = train_ML_model
    )

    save_ML_model = PythonOperator(
        task_id = 'save_ML_model',
        python_callable = save_ML_model
    )

    pull_spotify_data >> clean_spotify_data >> train_ML_model >> save_ML_model