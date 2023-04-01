# If you face import error try the following: python3 -m pip install psycopg2-binary
import psycopg2
import json
import csv
import pandas as pd

from sklearn.preprocessing import MinMaxScaler

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
    recommendation jsonb
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
    most_freq_artists jsonb
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
    tempo numeric,
    cluster int DEFAULT 0   
);
'''
cursor.execute(create_table_user)
cursor.execute(create_table_rec)
cursor.execute(create_table_viz)
cursor.execute(create_table_training)
cursor.execute("INSERT INTO user_data (username, playlist_id, email, password) VALUES ('test', 'https://open.spotify.com/playlist/7MFbySBZbklUth1B6MBCmF?si=2cf50b921c3641aa', 'test@test.com', 'test')")

new_file = pd.read_csv('Data/data.csv')
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
df_scaled = df_scaled.assign(cluster = 0)
df_scaled.to_csv('Data/song_data.csv', index = False)

file = open('Data/song_data.csv', 'r', encoding='utf-8')
ingest_data = '''COPY training_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
cursor.copy_expert(ingest_data, file)

# # Testing purposes
# cursor.execute("SELECT * FROM training_data LIMIT 10")
# row = cursor.fetchall()
# for i in row:
#     print(i)

# Redo schema DO NOT TOUCH
# cursor.execute('DROP SCHEMA public CASCADE;')
# cursor.execute('CREATE SCHEMA public;')
# cursor.execute('GRANT ALL ON SCHEMA public TO postgres;GRANT ALL ON SCHEMA public TO public;')


# Always incude at the end of the script
conn.close()