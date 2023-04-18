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
import openai

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
    
    def pullUserPlaylist(**kwargs): 
        ti = kwargs['ti']
        credentials = {"client_id": "dc329f61fb0e4f799151f42965ed6e83","client_secret": "ab55f38bf4da413ba8ba9c9af79609c2"}
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        conn = psycopg2.connect(database="spotify",
                user='postgres', password='admin123', 
                host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT username, email, playlist_id FROM user_data;")
        db_data = cursor.fetchall()
        conn.close()

        all_user_playlists = []
        pid_map = {}
        pid = ti.xcom_pull(task_ids='mergePlaylistData', key = 'last_pid')
        for (username, email, playlist_id) in db_data:
            results = sp.user_playlist(None, playlist_id, 'tracks')
            playlist_tracks_data = results['tracks']
            user_playlist = {'name': None, 'pid': None, 'tracks': []}
            pos = 0
            user_playlist['name'] = playlist_tracks_data['href']
            for song in playlist_tracks_data['items']:
                song_details = {}
                song_details['pos'] = pos
                pos += 1
                song_details['artist_name'] = song['track']['album']['artists'][0]['name']
                song_details['track_uri'] = song['track']['uri']
                song_details['track_name'] = song['track']['name']
                
                user_playlist['tracks'].append(song_details)

            # Adding pid (position of user's track)
            all_user_playlists.append(user_playlist)
            user_playlist['pid'] = pid
            pid_map[pid] = (username, user_playlist,)
            pid += 1
        print(pid_map)
        print(db_data)
        print(all_user_playlists)
        ti.xcom_push('pid_map', pid_map)
        ti.xcom_push('all_user_playlists', all_user_playlists)

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
        ti = kwargs['ti']
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
        ti.xcom_push('last_pid', all_playlists[-1]['pid'] + 1)
        with open('combined.json', 'w') as f:
            json.dump(combined_data, f)

    def playlistDataPreprocessing(**kwargs):
        ti = kwargs['ti']
        min_tracks_per_playlist=5
        min_track_frequency=10
        all_user_playlists = ti.xcom_pull(task_ids='pullUserPlaylist', key = 'all_user_playlists')

        with open('combined.json', 'r') as f:
            data = json.load(f)
            playlists = data['playlists']

        for playlist in all_user_playlists:
            playlists.append(playlist)

        # Filter out irrelevant information
        for playlist in playlists:
            playlist.pop('modified_at', None)
            for track in playlist['tracks']:
                track.pop('album_name', None)
                track.pop('duration_ms', None)
                track.pop('album_uri', None)
                track.pop('artist_uri', None)

        # Calculate the frequency of each track in the dataset
        track_frequencies = {}
        for playlist in playlists:
            for track in playlist['tracks']:
                track_uri = track['track_uri']
                if track_uri not in track_frequencies:
                    track_frequencies[track_uri] = 0
                track_frequencies[track_uri] += 1
            
        move_to_content_model = True
        playlists_to_process_diff = []
        for playlist in all_user_playlists:
            for song in playlist['tracks']:
                if song['track_uri'] in track_frequencies:
                    move_to_content_model = False
                    break
            if move_to_content_model == True:
                playlists_to_process_diff.append(playlist)
        # playlists = list(filter(lambda x: x not in playlists_to_process_diff, playlists))
        print(len(playlists))
        print(playlists_to_process_diff)
        ti.xcom_push('content_based_process', playlists_to_process_diff)
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

        print("Beginning saving")
        with open('util_matrix.npy', 'wb') as f:
            np.save(f, reconstructed_utility_matrix)
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

        conn = psycopg2.connect(database="spotify",
                user='postgres', password='admin123', 
                host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        
        openai.api_key = "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"
        ### EDIT TO FIT USER PLAYLIST ###
        user_mappings = ti.xcom_pull(task_ids='pullUserPlaylist', key = 'pid_map')

        print(user_mappings)
        for (pid, user_details) in user_mappings.items():

        #################################
            num_recommendations = 20

            original_row = feature_matrix.loc[int(pid)]
            reconstructed_row = util_matrix[int(pid)]

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
            print(recommended_songs_names)

            recommend_from_playlist_track_names = []
            for track in user_details[1]['tracks']:
                recommend_from_playlist_track_names.append(track['artist_name'] + ' by ' + track['track_name'])
            playlist_1 = str(recommend_from_playlist_track_names)
            playlist_2 = str(recommended_songs_names)
            
            explanations = []
            
            for song in recommended_songs_names:

                prompt = f"This is my playlist {playlist_1}. After hearing to all the songs in my playlist, why would you recommend me the song: {song}? \
                        Act as a recommender system. Give a short one sentence explanation. \
                        Only output the explanation and nothing else. Do not mention anything along the lines of 'Based on your playlist' "
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}]
                )

                response = response['choices'][0]['message']['content'].strip()
                explanations.append(response)

            cursor.execute('''INSERT INTO recommendation_data (username, recommendation, rec_explanation)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (username) DO UPDATE
                        SET recommendation = EXCLUDED.recommendation, rec_explanation = EXCLUDED.rec_explanation''',
                        (user_details[0], recommended_songs_names, explanations))
        conn.close()
  
    pullPlaylistData = PythonOperator(
        task_id = 'pullPlaylistData',
        python_callable = pullPlaylistData
    )

    pullUserPlaylist = PythonOperator(
        task_id = 'pullUserPlaylist',
        python_callable = pullUserPlaylist
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
    mergePlaylistData >> pullUserPlaylist >> playlistDataPreprocessing