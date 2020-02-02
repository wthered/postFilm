# movieId,imdbId,tmdbId
import json
import time
import urllib.error
import urllib.request

import pandas as pd
import psycopg2 as psy

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()

start = int(time.time())


def fetch(url):
	try:
		req = urllib.request.Request(
			url,
			data=None,
			headers={
				'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.01; Windows NT; Hotbar 4.1.8.0)'
			}
		)
		remote_data = urllib.request.urlopen(req)
	# print(remote_data.read().decode('utf-8'))
	except urllib.error.HTTPError as e:
		# Return code error (e.g. 404, 501, ...)
		print('HTTPError: {0} Link {1} does not exist(s)'.format(e.code, url))
		return {}
	except urllib.error.URLError as e:
		# Not an HTTP-specific error (e.g. connection refused)
		print('URLError: {1} Reason: {0}'.format(e.reason, e.strerror))
		return {}
	else:
		# 200
		data = remote_data.read().decode('utf-8')
		json_data = json.loads(data)
		if type(json_data) == dict:
			return json_data['movie_results']
		else:
			return []


movie_ratings = pd.read_csv('/tmp/ml-latest/links.csv', header=1)
for index, row in movie_ratings.iterrows():

	movie = str(round(row[1]))

	if len(row) == 3:
		links_query = "INSERT INTO movies.links values (%s,%s,%s) ON CONFLICT (title_id) DO NOTHING;"
		cur.execute(links_query, [row[0], row[1], row[2]])
		conn.commit()
		print(cur.query)
	else:
		url = "https://api.themoviedb.org/3/find/tt" + movie.zfill(
			7) + "?api_key=1cd7d832933a3f8cb0c956ac70964e5f&language=en-US&external_source=imdb_id"
		movie_data = fetch(url)
		print(movie_data)

print("That whole thing lasted {} seconds".format(int(time.time()) - start))
