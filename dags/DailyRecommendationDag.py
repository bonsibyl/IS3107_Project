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
 description='RecDaily',
 schedule_interval= '@daily',
 start_date=datetime(2023, 2, 2),
 catchup=False,
) as dag:

    def pullUserData(**kwargs):
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

    def pullSpotifyPlaylist(**kwargs):
        ti = kwargs['ti']
        db_data = ti.xcom_pull(task_ids = 'pullUserData', key = 'user_data')

        credentials = json.load(open('../Spotify_Scrape/authorization.json'))
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        all_playlist_data = {}
        for (username, email, playlist_id) in db_data:
            results = sp.user_playlist(username, playlist_id, 'tracks')
            playlist_tracks_data = results['tracks']
            playlist_tracks_id = []
            playlist_tracks_titles = []
            playlist_tracks_artists = []

            for track in playlist_tracks_data['items']:
                playlist_tracks_id.append(track['track']['id'])
                playlist_tracks_titles.append(track['track']['name'])
                # adds a list of all artists involved in the song to the list of artists for the playlist
                artist_list = []
                for artist in track['track']['artists']:
                    artist_list.append(artist['name'])
                playlist_tracks_artists.append(artist_list)
            features = sp.audio_features(playlist_tracks_id)
            features_df = pd.DataFrame(data=features, columns=features[0].keys())
            features_df['name'] = playlist_tracks_titles
            features_df['artists'] = playlist_tracks_artists
            features_df = features_df[['valence', 'acousticness', 'artists', 'danceability',
                                       'duration_ms','energy', 'id', 'instrumentalness', 'key',
                                       'liveness', 'loudness', 'mode', 'name', 'tempo']]
            all_playlist_data['username'] = features_df 
        ti.xcom_push('all_playlist_data', all_playlist_data)

    def cleanSpotifyData(**kwargs):
        ti = kwargs['ti']
        all_playlist_data = ti.xcom_pull(task_ids = 'pullSpotifyPlaylist', key='all_playlist_data')
        scaler = MinMaxScaler()
        for username, audio_feat_df in all_playlist_data:
            ref_df = all_playlist_data['username']
            ref_df_numeric = ref_df.select_dtypes(include=['int', 'float'])
            ref_df_scaled = scaler.fit_transform(ref_df_numeric)
            ref_df_scaled = pd.DataFrame(ref_df_scaled, columns=ref_df_numeric.columns)
            ref_df_scaled = pd.concat([ref_df_scaled, ref_df.select_dtypes(exclude=['int', 'float'])], axis=1)
            ref_df_scaled = ref_df_scaled[['valence', 'acousticness', 'artists', 'danceability',
                                       'duration_ms','energy', 'id', 'instrumentalness', 'key',
                                       'liveness', 'loudness', 'mode', 'name', 'tempo']]
            all_playlist_data['username'] = ref_df_scaled
        ti.xcom_push('cleaned_playlist_data', all_playlist_data)

    def retrieveMLModel(**kwargs):
        ti = kwargs['ti']
        s3 = boto3.client('s3')
        buffer = BytesIO()

        s3.download_fileobj('is3107', 'model', buffer)

        pkl_data = buffer.getvalue()
        model = pickle.load(pkl_data)

        ti.xcom_push('model', model)


    def makeRecommendationsAndUpdate(**kwargs):
        ti = kwargs['ti']
        cleaned_playlist_data = ti.xcom_pull(task_ids = 'cleanSpotifyData', key = 'cleaned_playlist_data')
        model = ti.xcom_pull(task_ids = 'retrieveMLModel', key = 'model')

        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        for username, playlist_features in cleaned_playlist_data.items():
            ##### Need to add based on how we want to do our model




            ####
            recommendation = json.dumps(model.predict())

            cursor.execute("UPDATE recommendation_data SET recommendations = ? WHERE username = ?", (recommendation, username))
        conn.close()
  
    pullUserData = PythonOperator(
        task_id = 'pullUserData',
        python_callable = pullUserData
    )

    pullSpotifyPlaylist = PythonOperator(
        task_id = 'pullSpotifyPlaylist',
        python_callable = pullSpotifyPlaylist
    )

    cleanSpotifyData = PythonOperator(
        task_id = 'cleanSpotifyData',
        python_callable = cleanSpotifyData
    )

    retrieveMLModel = PythonOperator(
        task_id = 'retrieveMLModel',
        python_callable = retrieveMLModel
    )

    makeRecommendationsAndUpdate= PythonOperator(
        task_id = 'makeRecommendationsAndUpdate',
        python_callable = makeRecommendationsAndUpdate
    )

    pullUserData, pullSpotifyPlaylist >> cleanSpotifyData >> makeRecommendationsAndUpdate
    retrieveMLModel >> makeRecommendationsAndUpdate