# If you face import error try the following: python3 -m pip install psycopg2-binary
import psycopg2
import json
import csv

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
    tempo numeric
);
'''
cursor.execute(create_table_user)
cursor.execute(create_table_rec)
cursor.execute(create_table_viz)
cursor.execute(create_table_training)

with open('Data/data.csv', 'r', encoding='utf-8') as file, open('Data/song_data.csv', 'w', encoding='utf-8', newline='') as output_file:
    # Skip header
    reader = csv.reader(file)
    writer = csv.writer(output_file)
    header = next(reader)
    del header[15:18]
    del header[7]
    writer.writerow(header)
    for row in reader:
        del row[15:18]
        del row[7]
        row[3] = json.dumps(row[3])
        writer.writerow(row)

file = open('Data/song_data.csv', 'r', encoding='utf-8')
ingest_data = '''COPY training_data FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',');'''
cursor.copy_expert(ingest_data, file)

# Testing purposes
# cursor.execute("SELECT * FROM song_data LIMIT 400")
# row = cursor.fetchall()
# for i in row:
#     print(i)

# Redo schema DO NOT TOUCH
# cursor.execute('DROP SCHEMA public CASCADE;')
# cursor.execute('CREATE SCHEMA public;')
# cursor.execute('GRANT ALL ON SCHEMA public TO postgres;GRANT ALL ON SCHEMA public TO public;')


# Always incude at the end of the script
conn.close()