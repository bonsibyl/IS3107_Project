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
from sklearn.preprocessing import MinMaxScaler

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

    def pullSpotifyPlaylist(**kwargs):
        ti = kwargs['ti']

        playlist_uri = 'spotify:playlist:37i9dQZEVXbK4gjvS1FjPY' #top50Singapore
        username = playlist_uri.split(':')[1]
        playlist_id = playlist_uri.split(':')[2]

        credentials = json.load(open('../Spotify_Scrape/authorization.json'))
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        results = sp.user_playlist(username, playlist_id, 'tracks')
        playlist_tracks_data = results['tracks']
        playlist_tracks_id = []
        playlist_tracks_titles = []
        playlist_tracks_artists = []
        playlist_tracks_years = []

        for track in playlist_tracks_data['items']:
            playlist_tracks_id.append(track['track']['id'])
            playlist_tracks_titles.append(track['track']['name'])
            release_date = track['track']['album']['release_date']
            release_year = release_date.split('-')[0]
            playlist_tracks_years.append(release_year)
            # adds a list of all artists involved in the song to the list of artists for the playlist
            artist_list = []
            for artist in track['track']['artists']:
                artist_list.append(artist['name'])
            playlist_tracks_artists.append(artist_list)
        features = sp.audio_features(playlist_tracks_id)
        features_df = pd.DataFrame(data=features, columns=features[0].keys())
        features_df['name'] = playlist_tracks_titles
        features_df['artists'] = playlist_tracks_artists
        features_df['year'] = playlist_tracks_years
        features_df = features_df[['valence','year', 'acousticness', 'artists', 'danceability',
                                    'duration_ms','energy', 'id', 'instrumentalness', 'key',
                                    'liveness', 'loudness', 'mode', 'name', 'tempo']]
        ti.xcom_push('new_training_data', features_df)

    def cleanSpotifyData(**kwargs):
        ti = kwargs['ti']
        new_training_data = ti.xcom_pull(task_ids = 'pullSpotifyPlaylist', key='new_training_data')

        scaler = MinMaxScaler()
        ref_df_numeric = new_training_data.select_dtypes(include=['int', 'float'])
        ref_df_scaled = scaler.fit_transform(ref_df_numeric)
        ref_df_scaled = pd.DataFrame(ref_df_scaled, columns=ref_df_numeric.columns)
        ref_df_scaled = pd.concat([ref_df_scaled, new_training_data.select_dtypes(exclude=['int', 'float'])], axis=1)
        ref_df_scaled['year'] = new_training_data['year']
        ref_df_scaled['key'] = new_training_data['key']
        ref_df_scaled['mode'] = new_training_data['mode']
        ref_df_scaled = ref_df_scaled.assign(cluster = 0)
        ref_df_scaled = ref_df_scaled[['valence', 'year', 'acousticness', 'artists', 'danceability',
                                    'duration_ms','energy', 'id', 'instrumentalness', 'key',
                                    'liveness', 'loudness', 'mode', 'name', 'tempo', 'cluster']]
        ti.xcom_push('cleaned_playlist_data', ref_df_scaled)

    def pullTrainingData(**kwargs):
        ti = kwargs['ti']
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True

        db_data = pd.read_sql('SELECT * FROM training_data;', conn)

        ti.xcom_push('current_training_data', db_data)
        conn.close()

    def trainMLModel(**kwargs):
        ti = kwargs['ti']
        # Pull playlist data
        new_training_data = ti.xcom_pull(task_ids = 'cleanSpotifyData', key = 'cleaned_playlist_data')
        current_training_data = ti.xcom_pull(task_ids = 'pullTrainingData', key = 'current_training_data')
        all_training_data = pd.concat([new_training_data, current_training_data], ignore_index = True)
        unique_training_data = all_training_data.drop_duplicates(subset='id', keep='first')
        unique_training_data['artists'] = [json.dumps(artists) for artists in unique_training_data['artists']]

        # Train model
        X = unique_training_data.select_dtypes(np.number)
        X.drop(['duration_ms', 'key', 'cluster'], axis = 1, inplace = True)
        kmeans = KMeans(n_clusters=10)
        cluster_labels = kmeans.fit_predict(X)

        unique_training_data['cluster'] = cluster_labels        
        # Pass model to xcom
        ti.xcom_push('model', kmeans)

        # Upload non-dup data to db
        conn = psycopg2.connect(database="spotify",
                user='postgres', password='admin123', 
                host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        values = unique_training_data.to_dict('records')

        cursor.executemany('''
            INSERT INTO training_data (valence, year, acousticness, artists, danceability, duration_ms, 
                                    energy, id, instrumentalness, key, liveness, loudness, mode, name, tempo, cluster)
            VALUES (%(valence)s, %(year)s, %(acousticness)s, %(artists)s, %(danceability)s, %(duration_ms)s, %(energy)s, 
                    %(id)s, %(instrumentalness)s, %(key)s, %(liveness)s, %(loudness)s, %(mode)s, %(name)s, %(tempo)s, %(cluster)s)
            ON CONFLICT (id) DO UPDATE SET cluster = excluded.cluster;
        ''', values)
        conn.close()


    def saveMLModel(**kwargs):
        ti = kwargs['ti']
        ### Pull model from xcom
        model = ti.xcom_pull(task_ids = 'train_ML_model', key = 'model')

        # AWS S3 Client
        s3 = boto3.client('s3')
        
        ### push to s3 bucket
        pickle.dump(model, open('model.pkl', 'wb'))
        s3.upload_file('model.pkl', 'is3107-spotify', 'model.pkl')

    pullSpotifyPlaylist = PythonOperator(
        task_id = 'pullSpotifyPlaylist',
        python_callable = pullSpotifyPlaylist
    )

    cleanSpotifyData = PythonOperator(
        task_id = 'cleanSpotifyData',
        python_callable = cleanSpotifyData
    )

    trainMLModel = PythonOperator(
        task_id = 'trainMLModel',
        python_callable = trainMLModel
    )

    saveMLModel = PythonOperator(
        task_id = 'save_ML_model',
        python_callable = saveMLModel
    )

    pullSpotifyPlaylist >> cleanSpotifyData
    cleanSpotifyData, pullTrainingData >> trainMLModel
    trainMLModel >> saveMLModel