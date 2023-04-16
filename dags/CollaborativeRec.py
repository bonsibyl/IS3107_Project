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

import os

from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse import csr_matrix, vstack
from scipy.sparse.linalg import svds

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
 'CollaborativeRecPipeline',
 default_args=default_args,
 description='CollabRecDaily',
 schedule_interval= None,
 start_date=datetime(2023, 2, 2),
 catchup=False,
) as dag:

    def pullPlaylistData(**kwargs):
        s3 = boto3.client('s3', aws_access_key_id="AKIAY73PMJYUU6QFJTOP", aws_secret_access_key="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")
        bucket_name = 'is3107-spotify'
        response = s3.list_objects_v2(Bucket=bucket_name)
        for obj in response.get('Contents', []):
            key = obj['Key']
            if (key[-1] == '/'):
                continue
            s3.download_file('is3107-spotify', key, key)

    def mergePlaylistData(**kwargs):
        all_playlists = []
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        for file in files:
            if file.endswith('.json'):
                # Read the second JSON file and extract the "playlists" array
                with open(file, 'r') as f:
                    data = json.load(f)
                    playlists = data['playlists']
                os.remove(file)
                # Append each playlist in the "playlists" array to the list created in step 1
                all_playlists.extend(playlists)
        combined_data = {
            'info': data['info'],  # Use the "info" object from the last file read
            'playlists': all_playlists
        }
        with open('combined.json', 'w') as f:
            json.dump(combined_data, f)

    def playlistDataPreprocessing(**kwargs):
        ti = kwargs['ti']
        min_tracks_per_playlist=5
        min_track_frequency=10

        with open('combined.json', 'r') as f:
            data = json.load(f)
            playlists = data['playlists']

        # Filter out irrelevant information
        for playlist in playlists:
            playlist.pop('modified_at', None)
            for track in playlist['tracks']:
                track.pop('album_name', None)
                track.pop('duration_ms', None)
                track.pop('album_uri', None)
                track.pop('artist_uri', None)

        # Filter playlists with fewer tracks than the minimum threshold
        playlists = [playlist for playlist in playlists if len(playlist['tracks']) >= min_tracks_per_playlist]

        # Calculate the frequency of each track in the dataset
        track_frequencies = {}
        for playlist in playlists:
            for track in playlist['tracks']:
                track_uri = track['track_uri']
                if track_uri not in track_frequencies:
                    track_frequencies[track_uri] = 0
                track_frequencies[track_uri] += 1

        # Filter out tracks with a frequency lower than the minimum threshold
        for playlist in playlists:
            playlist['tracks'] = [track for track in playlist['tracks'] if track_frequencies[track['track_uri']] >= min_track_frequency]
        with open('cleaned_combined.json', 'w') as f:
            json.dump(playlists, f)

    def createFeatureMatrix(**kwargs):
        ti = kwargs['ti']
        with open('cleaned_combined.json', 'r') as f:
            data = json.load(f)
            cleaned_playlist_data = data
        os.remove('cleaned_combined.json')

        processed_data = []
        for playlist in cleaned_playlist_data:
            pid = playlist['pid']
            for track in playlist['tracks']:
                track_data = {
                    'user_id': pid,
                    'track': track['track_uri'],
                }
                processed_data.append(track_data)
        print("finished processing")
        df = pd.DataFrame(processed_data)
        feature_matrix = pd.crosstab(df['user_id'], df['track'])
        # Convert the utility matrix to a sparse CSR matrix
        sparse_utility_matrix = csr_matrix(feature_matrix)

        # apply the svds function to perform truncated SVD:
        sparse_utility_matrix = sparse_utility_matrix.astype(np.float32)
        U, sigma, Vt = svds(sparse_utility_matrix, k =15)

        # Since sigma is returned as a 1D array, convert it to a diagonal matrix
        sigma_matrix = np.diag(sigma)

        # Reconstruct the utility matrix
        reconstructed_utility_matrix = np.dot(np.dot(U, sigma_matrix), Vt)

        # Values are very small, use apply minmaxscaler
        reconstructed_utility_matrix_scaled = (reconstructed_utility_matrix - reconstructed_utility_matrix.min()) / (reconstructed_utility_matrix.max() - reconstructed_utility_matrix.min())
        print("Beginning saving")
        with open('util_matrix.npy', 'wb') as f:
            np.save(f, reconstructed_utility_matrix_scaled)
        print("finish saving")
        feature_matrix.to_csv('feature_matrix.csv', index = False)

    def makeRecommendationsAndUpdate(**kwargs):
        ti = kwargs['ti']
        with open('util_matrix.npy', 'rb') as f:
            util_matrix = np.load(f)
        feature_matrix = pd.read_csv('feature_matrix.csv')
        os.remove('feature_matrix.csv')
        os.remove('util_matrix.npy')

        with open('combined.json', 'r') as f:
            data = json.load(f)
            playlists = data['playlists']
        os.remove('combined.json')
        
        ### EDIT TO FIT USER PLAYLIST ###
        pid = 0

        #################################
        num_recommendations = 20

        original_row = feature_matrix.loc[pid]
        reconstructed_row = util_matrix[pid]

        recommendations = []
        for track, original_presence in zip(original_row.index, original_row):
            if original_presence == 0:  # We only consider tracks not already in the playlist
                # Getting track name, similarity matrix
                recommendations.append((track, reconstructed_row[feature_matrix.columns.get_loc(track)]))

        sorted_recommendations = sorted(recommendations, key=lambda x: x[1], reverse=True)
        recommended_track_uris = [track_uri for track_uri, _ in sorted_recommendations[:num_recommendations]]
        print(recommended_track_uris)

        all_tracks = {}

        for playlist in playlists:
            for track in playlist['tracks']:
                all_tracks[track['track_uri']] = [track['artist_name'], track['track_name'], playlist['name']]

        recommended_songs_names = []
        for recommended_track in recommended_track_uris:
            recommended_songs_names.append(all_tracks[recommended_track][1] + ' by ' + all_tracks[recommended_track][0])
        recommended_songs_names

        print(recommended_songs_names)
        
        conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        # for username, playlist_features in cleaned_playlist_data.items():
        #     ##### Need to add based on how we want to do our model
        #     playlist_features = pd.DataFrame.from_dict(playlist_features)
        #     value_vector = np.mean(playlist_features, axis = 0)
        #     print(value_vector)
        #     value_vector.drop(['year', 'duration_ms', 'key', 'mode'], inplace = True)
        #     adjusted_value_vector = value_vector.values.reshape(1, -1)
        #     predicted_cluster = model.predict(adjusted_value_vector)
        #     predicted_songs = song_data.loc[song_data['cluster']== predicted_cluster[0]].sample(n=20).values.tolist()
        #     new_recommendations = list(set(map(tuple,predicted_songs)) - set(song_data))
        #     ####
        #     cursor.execute('''INSERT INTO recommendation_data (username, recommendation)
        #                 VALUES (%s, %s)
        #                 ON CONFLICT (username) DO UPDATE
        #                 SET recommendation = EXCLUDED.recommendation''', (username, json.dumps(new_recommendations)))
        conn.close()
  
    pullPlaylistData = PythonOperator(
        task_id = 'pullPlaylistData',
        python_callable = pullPlaylistData
    )

    mergePlaylistData = PythonOperator(
        task_id = 'mergePlaylistData',
        python_callable = mergePlaylistData
    )

    playlistDataPreprocessing = PythonOperator(
        task_id = 'playlistDataPreprocessing',
        python_callable = playlistDataPreprocessing
    )

    createFeatureMatrix = PythonOperator(
        task_id = 'createFeatureMatrix',
        python_callable = createFeatureMatrix
    )

    makeRecommendationsAndUpdate= PythonOperator(
        task_id = 'makeRecommendationsAndUpdate',
        python_callable = makeRecommendationsAndUpdate
    )

    pullPlaylistData >> mergePlaylistData >> playlistDataPreprocessing >> createFeatureMatrix >> makeRecommendationsAndUpdate
