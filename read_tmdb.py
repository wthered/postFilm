# Reads moviesID from http://files.tmdb.org/p/exports/movie_ids_04_28_2017.json.gz
# and inserts into movies.tmdb

import psycopg2 as psy
import json

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()

for line in open('/tmp/movie_ids_07_08_2018.json', 'r').readlines():
	line = line.strip('\n')
	data = json.loads(line)
	if data['adult'] is False:
		query = "INSERT INTO movies.tmdb (id, title, popularity, video) VALUES (%s,%s,%s,%s)"
		cur.execute(query, [data['id'], data['original_title'], data['popularity'], data['video']])
		conn.commit()
		print("Inserted", data['id'], "as", data['original_title'])
	# print(data['id'], "is", data['original_title'])
