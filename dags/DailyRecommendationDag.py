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


# from sklearn.cluster import KMeans
# from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from sklearn.metrics import euclidean_distances
from scipy.spatial.distance import cdist

default_args = {
 'owner': 'airflow',
}

with DAG (
 'RecommendationDailyPipeline',
 default_args=default_args,
 description='RecDaily',
 schedule_interval= None,
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

    def pullSongData(**kwargs):
        ti = kwargs['ti']
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        song_data = pd.read_sql('SELECT name, artists, year, cluster FROM training_data;', conn)
        ti.xcom_push('song_data', song_data.to_dict())

    def pullSpotifyPlaylist(**kwargs):
        ti = kwargs['ti']
        db_data = ti.xcom_pull(task_ids = 'pullUserData', key = 'user_data')

        credentials = {"client_id": "dc329f61fb0e4f799151f42965ed6e83","client_secret": "ab55f38bf4da413ba8ba9c9af79609c2"}
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
            all_playlist_data[username] = features_df.to_dict()
        ti.xcom_push('all_playlist_data', all_playlist_data)

    def cleanSpotifyData(**kwargs):
        ti = kwargs['ti']
        all_playlist_data = ti.xcom_pull(task_ids = 'pullSpotifyPlaylist', key='all_playlist_data')

        scaler = MinMaxScaler()
        for username, audio_feat_df in all_playlist_data.items():
            ref_df = pd.DataFrame.from_dict(audio_feat_df)
            ref_df_numeric = ref_df[['valence', 'acousticness', 'danceability', 'duration_ms', 'energy', 'instrumentalness','liveness', 'loudness', 'tempo']]
            ref_df_scaled = scaler.fit_transform(ref_df_numeric)
            ref_df_scaled = pd.DataFrame(ref_df_scaled, columns=ref_df_numeric.columns)
            ref_df_scaled.reset_index(inplace = True)
            ref_df.reset_index(inplace = True)
            ref_df_scaled['year'] = ref_df['year'].astype(int)
            ref_df_scaled['artists'] = ref_df['artists'].astype(str)
            ref_df_scaled['key'] = ref_df['key'].astype(int)
            ref_df_scaled['mode'] = ref_df['mode'].astype(int)
            ref_df_scaled['name'] = ref_df['name'].astype(str)
            ref_df_scaled['id'] = ref_df['id'].astype(str)
            ref_df_scaled = ref_df_scaled[['valence', 'year', 'acousticness', 'artists', 'danceability',
                                        'duration_ms','energy', 'id', 'instrumentalness', 'key',
                                        'liveness', 'loudness', 'mode', 'name', 'tempo']]
            all_playlist_data[username] = ref_df_scaled.to_dict()
        ti.xcom_push('cleaned_playlist_data', all_playlist_data)

    def retrieveMLModel(**kwargs):
        ti = kwargs['ti']

        s3 = boto3.client('s3', aws_access_key_id="AKIAY73PMJYUU6QFJTOP", aws_secret_access_key="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")
        s3.download_file('is3107-spotify', 'model.pkl', 'model.pkl')

    def makeRecommendationsAndUpdate(**kwargs):
        ti = kwargs['ti']
        cleaned_playlist_data = ti.xcom_pull(task_ids = 'cleanSpotifyData', key = 'cleaned_playlist_data')
        song_data = ti.xcom_pull(task_ids = 'pullSongData', key = 'song_data')
        model = pickle.load(open('model.pkl', 'rb'))

        song_data = pd.DataFrame.from_dict(song_data)

        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        for username, playlist_features in cleaned_playlist_data.items():
            ##### Need to add based on how we want to do our model
            playlist_features = pd.DataFrame.from_dict(playlist_features)
            value_vector = np.mean(playlist_features, axis = 0)
            print(value_vector)
            value_vector.drop(['year', 'duration_ms', 'key', 'mode'], inplace = True)
            adjusted_value_vector = value_vector.values.reshape(1, -1)
            predicted_cluster = model.predict(adjusted_value_vector)
            predicted_songs = song_data.loc[song_data['cluster']== predicted_cluster[0]].sample(n=20).values.tolist()
            new_recommendations = list(set(map(tuple,predicted_songs)) - set(song_data))
            ####
            cursor.execute('''INSERT INTO recommendation_data (username, recommendation)
                        VALUES (%s, %s)
                        ON CONFLICT (username) DO UPDATE
                        SET recommendation = EXCLUDED.recommendation''', (username, json.dumps(new_recommendations)))
        conn.close()
  
    pullUserData = PythonOperator(
        task_id = 'pullUserData',
        python_callable = pullUserData
    )

    pullSpotifyPlaylist = PythonOperator(
        task_id = 'pullSpotifyPlaylist',
        python_callable = pullSpotifyPlaylist
    )

    pullSongData = PythonOperator(
        task_id = 'pullSongData',
        python_callable = pullSongData
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

    pullUserData >> pullSpotifyPlaylist >> cleanSpotifyData >> makeRecommendationsAndUpdate
    pullSongData >> makeRecommendationsAndUpdate
    retrieveMLModel >> makeRecommendationsAndUpdate