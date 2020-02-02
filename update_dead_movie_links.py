# Read the double tmdb links and updates table
# Updates movies.links table by quering theMovieDatabase API

import json
import urllib.error
import urllib.request

import psycopg2 as psy

aluminium_IP = "192.168.1.13"
aluminium_shared = "10.42.0.13"

conn = psy.connect(host=aluminium_IP, database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def get_all_movies():
	query = "SELECT * FROM movies.links WHERE tmdb in (SELECT tmdb FROM movies.links GROUP BY tmdb HAVING count(tmdb) > 1)"
	cur.execute(query, [])
	return cur.fetchall()


def fetch(internet):
	try:
		browser = 'Mozilla/4.0 (compatible; MSIE 5.0; Windows 98;)'
		req = urllib.request.Request(
			internet,
			data=None,
			headers={
				'User-Agent': browser
			}
		)

		remote_data = urllib.request.urlopen(req)
		data = remote_data.read().decode('utf-8')
		json_data = json.loads(data)
	except urllib.error.HTTPError as e:
		# Return code error (e.g. 404, 501, ...)
		print('HTTPError: {0} Link {1} does not exist(s)'.format(e.code, internet))
		pass
	except urllib.error.URLError as e:
		# Not an HTTP-specific error (e.g. connection refused)
		print('URLError: {1} Reason: {0}'.format(e.reason, e.strerror))
		pass
	else:
		# print('{0}'.format(json_data))
		return json_data


credentials = '?api_key=1cd7d832933a3f8cb0c956ac70964e5f&language=en-US&external_source=imdb_id'

for movie in get_all_movies():
	movie_info = fetch('https://api.themoviedb.org/3/find/tt' + str(movie[1]).zfill(7) + credentials)
	try:
		new_movie = movie_info['movie_results'][0]['id']
		# print('tt{0}\t{1}'.format(movie[1], movie_info['movie_results'][0]['id']))
		last_query = "UPDATE movies.links SET tmdb = %s WHERE imdb = %s"
		cur.execute(last_query, [movie_info['movie_results'][0]['id'], movie[1]])
		conn.commit()
		print(cur.query)
	except IndexError:
		print('tt{0} not found in theMovieDatabase'.format(str(movie[1]).zfill(7)))
