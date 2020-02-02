import json
import math
import urllib.error
import urllib.request

import pandas as pd
import psycopg2 as psy

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def fetch(url):
	try:
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.01; Windows NT; Hotbar 4.1.8.0)'
		req = urllib.request.Request(
			url,
			data=None,
			headers={
				'User-Agent': user_agent
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
		# print(json_data['id'])

		if type(json_data) == dict and 'id' in dict():
			return int(json_data['id'])
		else:
			return 0


movie_ratings = pd.read_csv('/tmp/ml-latest/links.csv', header=None)
for index, line in movie_ratings.iterrows():
	# if len(line[:-1].split(',')) != 3:
	# 	data = line[:-1].split(',')
	# 	print("Line 57", data[0], "\t", data[1], "\t", data[2])
	# 	break
	# else:
	# 	(title, imdb, movie) = line[:-1].split(',')

	title = line[0]
	imdb = str(round(int(line[1]))).zfill(7)
	movie = line[2]

	# print(title, imdb, movie)

	if math.isnan(movie):
		link = "https://api.themoviedb.org/3/find/tt" + imdb + "?api_key=1cd7d832933a3f8cb0c956ac70964e5f&language=en-US&external_source=imdb_id"

		movie = fetch(link)
		# print("Requesting", link)
		print(movie)
	else:
		movie = line[2]

	links_query = "INSERT INTO movies.links values (%s,%s,%s) ON CONFLICT (title_id) DO NOTHING;"
	cur.execute(links_query, [title, imdb, movie])
	# print("Inserting movie", title, "or tt" + str(int(imdb)).zfill(7), "as", movie)
	print(cur.query)
	conn.commit()
#   print(title, imdb, movie)
