# Reads Titles from database and updates titles

import time
import urllib.error
import urllib.request

from credentials import *
from database import Database


conn = Database()


def read_links(limit, movie_page):
	conn.last_query = "SELECT * FROM links ORDER BY entry_id OFFSET %s LIMIT %s"
	conn.select([
		            movie_page * limit,
		            limit
	            ], True, True)
	return conn.results


def fetch(url):
	try:
		req = urllib.request.Request(url, data=None, headers=http_headers)

		remote_data = urllib.request.urlopen(req)
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
		return json_data['movie_results'][0]


movies = 1000
page = 1
start = int(time.time())

for row in read_links(movies, page):
	credentials = "?api_key=1cd7d832933a3f8cb0c956ac70964e5f&language=en-US&external_source=imdb_id"
	link = "https://api.themoviedb.org/3/find/tt" + str(row[1]).zfill(7) + credentials
	if row[2] is not None:
		movie_data = fetch(link)

		if 'release_date' not in movie_data or len(movie_data['release_date']) == 0:
			(year,
			 month,
			 day) = (None,
			         None,
			         None)
			movie_data['release_date'] = None
		else:
			(year,
			 month,
			 day) = movie_data['release_date'].split('-')

		if movie_data['poster_path'] is None:
			poster = None
		else:
			poster = "https://image.tmdb.org/t/p/original" + movie_data['poster_path']

		conn.last_query = "UPDATE movies SET title = %s, original_title = %s, release_date = %s, poster = %s, rating = %s, votes = %s, plot = %s, updated_at = %s WHERE id = %s"
		parameters = [
			movie_data['title'],
			movie_data['original_title'],
			movie_data['release_date'],
			poster,
			movie_data['vote_count'],
			movie_data['vote_average'],
			movie_data['overview'],
			int(time.time()),
			row[0]
		]
		conn.insert(parameters, False, True)
		print("Updated", row[0], "with title", movie_data['original_title'], "(" + year + ") as tt" + str(row[1]).zfill(7))
# print(cur.query)

print("That whole thing lasted {} seconds".format(int(time.time()) - start))
