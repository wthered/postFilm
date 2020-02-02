# Reads Titles from database and updates titles according to the movie database link
# https://api.themoviedb.org/3/movie/550?api_key=1cd7d832933a3f8cb0c956ac70964e5f

import psycopg2 as psy
import urllib.error
import urllib.request
import json
import time

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def read_links(limit, movie_page):
	last_query = "SELECT * FROM movies.links ORDER BY title_id OFFSET %s LIMIT %s"
	cur.execute(last_query, [movie_page * limit, limit])
	return cur.fetchall()


def fetch(url):
	try:
		agnes = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'
		req = urllib.request.Request(
			url,
			data=None,
			headers={
				'User-Agent': agnes
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
		return json.loads(data)


movies = 500
page = 0
start = int(time.time())

for row in read_links(movies, page):
	credentials = "?api_key=1cd7d832933a3f8cb0c956ac70964e5f"
	link = "https://api.themoviedb.org/3/movie/" + str(row[2]) + credentials
	if row[2] is not None:
		movie_data = fetch(link)

		if 'release_date' not in movie_data or len(movie_data['release_date']) == 0:
			(year, month, day) = (None, None, None)
			movie_data['release_date'] = None
		else:
			(year, month, day) = movie_data['release_date'].split('-')

		if movie_data['poster_path'] is None:
			poster = None
		else:
			poster = "https://image.tmdb.org/t/p/w500" + movie_data['poster_path']

		movie_query = "UPDATE movies.titles SET title = %s, year = %s, plot = %s, poster = %s, updated = %s, critics = %s, rating = %s, budget = %s, original_lang = %s, original_title = %s, release_date = %s, revenue = %s, runtime = %s, tagline = %s WHERE title_id = %s"
		parameters = [movie_data['title'],
		              year,
		              movie_data['overview'],
		              poster,
		              int(time.time()),
		              movie_data['vote_count'],
		              movie_data['vote_average'],
		              movie_data['budget'],
		              movie_data['original_language'],
		              movie_data['original_title'],
		              movie_data['release_date'],
		              movie_data['revenue'],
		              movie_data['runtime'],
		              movie_data['tagline'],
		              row[0]
		              ]
		cur.execute(movie_query, parameters)
		conn.commit()
		print("Updated", row[0], "as", movie_data['original_title'], "(" + year + ") or tt" + str(row[1]).zfill(7))

print("That whole thing lasted {} seconds".format(int(time.time()) - start))
