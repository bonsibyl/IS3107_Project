# If you face import error try the following: python3 -m pip install psycopg2-binary
import psycopg2
import json

conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432'
)
  
conn.autocommit = True
cursor = conn.cursor()

# Created already. Here for future reference

# create_table_playlist = '''CREATE TABLE playlist_data(name varchar(80), num_holdouts int, \
# pid int PRIMARY KEY NOT NULL, num_tracks int, tracks jsonb);'''
# cursor.execute(create_table_playlist)

# create_table_data = '''CREATE TABLE song_data(valence numeric, year int, \
# acousticness numeric, artists text[], danceability numeric, duration_ms numeric, \
# energy numeric, explicit int, id varchar(30) PRIMARY KEY NOT NULL, instrumentalness numeric, \
# key int, liveness numeric, loudness numeric, mode int, name varchar(150), popularity int, \
# release_data varchar(20), speechiness numeric, tempo numeric);
# '''
# cursor.execute(create_table_data)

# create_table_artist = '''CREATE TABLE artist_data(mode int, count int, acousticness numeric, \
# artists varchar(300), danceability numeric, duration_ms numeric, energy numeric, instrumentalness numeric, \
# liveness numeric, loudness numeric, speechiness numeric, tempo numeric, valence numeric, popularity numeric, \
# key int);
# '''
# cursor.execute(create_table_artist)

# create_table_genres = '''CREATE TABLE genre_data(mode int, genres varchar(50), \
# acousticness numeric, danceability numeric, duration_ms numeric, energy numeric, \
# instrumentalness numeric, liveness numeric, loudness numeric, speechiness numeric, \
# tempo numeric, valence numeric, popularity numeric, key int);
# '''
# cursor.execute(create_table_genres)

# create_table_year = '''CREATE TABLE year_data(mode int, year int, acousticness numeric, \
# danceability numeric, duration_ms numeric, energy numeric, instrumentalness numeric, \
# liveness numeric, loudness numeric, speechiness numeric, tempo numeric, valence numeric, \
# popularity numeric, key int);
# '''
# cursor.execute(create_table_year)

# create_table_genre_artist = '''CREATE TABLE genre_artist_data(genres text[], artists varchar(300), \
# acousticness numeric, danceability numeric, duration_ms numeric, energy numeric, \
# instrumentalness numeric, liveness numeric, loudness numeric, speechiness numeric, \
# tempo numeric, valence numeric, popularity numeric, key int, mode int, count int);
# '''
# cursor.execute(create_table_genre_artist)

# file = open('data_by_artist.csv', 'r', encoding='utf-8')
# ingest_data = '''COPY artist_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
# cursor.copy_expert(ingest_data, file)

# file = open('data_by_genres.csv', 'r', encoding='utf-8')
# ingest_data = '''COPY genre_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
# cursor.copy_expert(ingest_data, file)

# file = open('data_by_year.csv', 'r', encoding='utf-8')
# ingest_data = '''COPY year_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
# cursor.copy_expert(ingest_data, file)
    
# file = open('data_w_genres.csv', 'r', encoding='utf-8')
# ingest_data = '''COPY genre_artist_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
# cursor.copy_expert(ingest_data, file)

# with open('million_data_sample.json', 'r') as file:
#     json_data = file.read()
# data = json.loads(json_data)
# playlist_data = [(playlist["name"], playlist["num_holdouts"], playlist['pid'], playlist['num_tracks'], json.dumps(playlist['tracks'])) for playlist in data["playlists"]]
# cursor.executemany("INSERT INTO playlist_data (name, num_holdouts, pid, num_tracks, tracks) VALUES (%s, %s, %s, %s, %s)", playlist_data)

# cursor.execute("SELECT * FROM playlist_data")
# row = cursor.fetchall()
# for i in row:
#     print("test")

# Redo schema DO NOT TOUCH
# cursor.execute('DROP SCHEMA public CASCADE;')
# cursor.execute('CREATE SCHEMA public;')
# cursor.execute('GRANT ALL ON SCHEMA public TO postgres;GRANT ALL ON SCHEMA public TO public;')


# Always incude at the end of the script
conn.close()