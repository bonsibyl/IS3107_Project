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

default_args = {
 'owner': 'airflow',
}

with DAG (
 'MLWeeklyPipeline',
 default_args=default_args,
 description='MLWeekly',
 schedule_interval= None,
 start_date=datetime(2023, 2, 2),
 catchup=False,
) as dag:

    def pullSpotifyPlaylist(**kwargs):
        ti = kwargs['ti']

        playlist_uri = 'spotify:playlist:37i9dQZEVXbK4gjvS1FjPY' #top50Singapore
        username = playlist_uri.split(':')[1]
        playlist_id = playlist_uri.split(':')[2]

        credentials = {"client_id": "dc329f61fb0e4f799151f42965ed6e83","client_secret": "ab55f38bf4da413ba8ba9c9af79609c2"}
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
        ti.xcom_push('new_training_data', features_df.to_dict())

    def cleanSpotifyData(**kwargs):
        ti = kwargs['ti']
        new_training_data = ti.xcom_pull(task_ids = 'pullSpotifyPlaylist', key='new_training_data')
        new_training_data = pd.DataFrame.from_dict(new_training_data)
        scaler = MinMaxScaler()
        ref_df_numeric = new_training_data[['valence', 'acousticness', 'danceability', 'duration_ms', 'energy', 'instrumentalness','liveness', 'loudness', 'tempo']]
        ref_df_scaled = scaler.fit_transform(ref_df_numeric)
        ref_df_scaled = pd.DataFrame(ref_df_scaled, columns=ref_df_numeric.columns)
        ref_df_scaled.reset_index(inplace = True)
        new_training_data.reset_index(inplace = True)
        ref_df_scaled['year'] = new_training_data['year'].astype(int)
        ref_df_scaled['artists'] = new_training_data['artists'].astype(str)
        ref_df_scaled['key'] = new_training_data['key'].astype(int)
        ref_df_scaled['mode'] = new_training_data['mode'].astype(int)
        ref_df_scaled['name'] = new_training_data['name'].astype(str)
        ref_df_scaled['id'] = new_training_data['id'].astype(str)
        ref_df_scaled = ref_df_scaled.assign(cluster = 0)
        ref_df_scaled = ref_df_scaled[['valence', 'year', 'acousticness', 'artists', 'danceability',
                                    'duration_ms','energy', 'id', 'instrumentalness', 'key',
                                    'liveness', 'loudness', 'mode', 'name', 'tempo', 'cluster']]
        ti.xcom_push('cleaned_playlist_data', ref_df_scaled.to_dict())

    def pullTrainingData(**kwargs):
        ti = kwargs['ti']
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True

        db_data = pd.read_sql('SELECT * FROM training_data;', conn)

        ti.xcom_push('current_training_data', db_data.to_dict())
        conn.close()

    def trainMLModel(**kwargs):
        ti = kwargs['ti']
        # Pull playlist data
        new_training_data = ti.xcom_pull(task_ids = 'cleanSpotifyData', key = 'cleaned_playlist_data')
        current_training_data = ti.xcom_pull(task_ids = 'pullTrainingData', key = 'current_training_data')
        new_training_data = pd.DataFrame.from_dict(new_training_data)
        current_training_data = pd.DataFrame.from_dict(current_training_data)

        all_training_data = pd.concat([new_training_data, current_training_data], ignore_index = True)
        unique_training_data = all_training_data.drop_duplicates(subset='id', keep='first')
        unique_training_data['artists'] = [json.dumps(artists) for artists in unique_training_data['artists']]

        # Train model
        # X = unique_training_data.select_dtypes(np.number)
        X = unique_training_data
        X.drop(['year', 'duration_ms', 'key', 'cluster', 'id', 'mode', 'name', 'artists'], axis = 1, inplace = True)
        X.reset_index()
        X[~X.isin([np.nan, np.inf, -np.inf]).any(1)]
        kmeans = KMeans(n_clusters=10)
        cluster_labels = kmeans.fit_predict(X)

        unique_training_data['cluster'] = cluster_labels        
        # Serialize model
        pickle.dump(kmeans, open('model.pkl', 'wb'))

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
        # model = ti.xcom_pull(task_ids = 'train_ML_model', key = 'model')

        # AWS S3 Client
        s3 = boto3.client('s3', aws_access_key_id="AKIAY73PMJYUU6QFJTOP", aws_secret_access_key="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")
        
        ### push to s3 bucket
        s3.upload_file('model.pkl', 'is3107-spotify', 'model.pkl')

    pullSpotifyPlaylist = PythonOperator(
        task_id = 'pullSpotifyPlaylist',
        python_callable = pullSpotifyPlaylist
    )

    cleanSpotifyData = PythonOperator(
        task_id = 'cleanSpotifyData',
        python_callable = cleanSpotifyData
    )

    pullTrainingData = PythonOperator(
        task_id = 'pullTrainingData',
        python_callable = pullTrainingData
    )

    trainMLModel = PythonOperator(
        task_id = 'trainMLModel',
        python_callable = trainMLModel
    )

    saveMLModel = PythonOperator(
        task_id = 'save_ML_model',
        python_callable = saveMLModel
    )

    pullSpotifyPlaylist >> cleanSpotifyData >> trainMLModel >> saveMLModel
    pullTrainingData >> trainMLModel