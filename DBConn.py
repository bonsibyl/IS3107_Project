# If you face import error try the following: python3 -m pip install psycopg2-binary
import psycopg2
  
conn = psycopg2.connect(database="spotify",
                        user='postgres', password='admin123', 
                        host='is3107-proj.cieo7a0vgrlz.ap-southeast-1.rds.amazonaws.com', port='5432'
)
  
conn.autocommit = True
cursor = conn.cursor()

# Created already. Here for future reference

# create_table_playlist = '''CREATE TABLE playlist_data(name varchar(30), num_holdouts int, \
# pid int PRIMARY KEY NOT NULL, num_tracks int, tracks jsonb);'''
# cursor.execute(create_table_playlist)

# create_table_data = '''CREATE TABLE song_data(valence numeric, year int, \
# acousticness numeric, artists text[], danceability numeric, duration_ms int, \
# energy numeric, explicit int, id varchar(30) PRIMARY KEY NOT NULL, instrumentalness numeric);
# '''
# cursor.execute(create_table_data)

# create_table_artist = '''CREATE TABLE artist_data(mode int, count int, acousticness numeric, \
# artists varchar(80), danceability numeric, duration_ms int, energy numeric, instrumentalness numeric, \
# liveness numeric, loudness numeric)
# '''
# cursor.execute(create_table_artist)

# create_table_genres = '''CREATE TABLE genre_data(mode int, genres varchar(30), \
# acousticness numeric, danceability numeric, duration_ms int, energy numeric, \
# instrumentalness numeric, liveness numeric, loudness numeric, speechiness numeric)
# '''

# cursor.execute(create_table_genres)
# create_table_year = '''CREATE TABLE year_data(mode int, year int, acousticness numeric, \
# danceability numeric, duration_ms int, energy numeric, instrumentalness numeric, \
# liveness numeric, loudness numeric, speechiness numeric)
# '''

# cursor.execute(create_table_year)
# create_table_genre_artist = '''CREATE TABLE genre_artist_data(genres text[], artists varchar(80), \
# acousticness numeric, danceability numeric, duration_ms int, energy numeric, \
# instrumentalness numeric, liveness numeric, loudness numeric, speechiness numeric)
# '''
# cursor.execute(create_table_genre_artist)



# Always incude at the end of the script
conn.close()