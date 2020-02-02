# Reads Titles from database and updates titles according to the movie database link
# https://api.themoviedb.org/3/movie/550?api_key=1cd7d832933a3f8cb0c956ac70964e5f

import psycopg2 as psy
import urllib.error
import urllib.request
import json
import time
import random

conn = psy.connect(host="192.168.1.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def read_links():
	last_query = "SELECT * FROM movies.links ORDER BY title_id OFFSET %s LIMIT %s"
	cur.execute(last_query, [movies * page + deep, movies])
	return cur.fetchall()


def fetch(movie_db, title_id):
	link = "https://api.themoviedb.org/3/movie/" + movie_db + "?api_key=1cd7d832933a3f8cb0c956ac70964e5f"
	try:
		mozilla = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
		req = urllib.request.Request(
			link,
			data=None,
			headers={
				'User-Agent': mozilla
			}
		)

		remote_data = urllib.request.urlopen(req)
	# print(remote_data.read().decode('utf-8'))
	except urllib.error.HTTPError as e:
		# Return code error (e.g. 404, 501, ...)
		print('\n*****\nHTTPError: {0} ({2})\nLink {1}\nTitle_ID: Updated {4} as {3}\n*****'.format(e.code, link, e.msg,
		                                                                                            title_id, movie_db))
		random_movie = int(random.randint(0, len(all_database_movies)))
		new_movie_data = fetch(str(all_database_movies[random_movie]), title_id)
		last_query = "UPDATE movies.links SET imdb = %s, tmdb = %s WHERE tmdb = %s"
		last_movie = str(new_movie_data['imdb_id'])
		try:
			temp_movie = last_movie[2:]
			if temp_movie[0] == '0':
				while temp_movie[0] == "0":
					# print('Line 47: {0}'.format(temp_movie))
					temp_movie = temp_movie[1:]
			last_movie = temp_movie
			cur.execute(last_query, [last_movie, new_movie_data['id'], int(movie_db)])
			conn.commit()
			# print('Line 45:\t{0} Updated {2} as {1}'.format(cur.statusmessage, new_movie_data['id'], movie_db))
			# print('DELETE from movies.tmdb WHERE id = {0}'.format(new_movie_data['id']))
		except psy.IntegrityError as integrityErr:
			print('Line 48: Error {0} with code {1}'.format(integrityErr.pgerror, integrityErr.pgcode))
		except psy.DataError as data_error:
			if not last_movie:
				print("** Line 51: {0} is NULL".format(last_movie))
				random_movie = int(random.randint(0, len(all_database_movies)))
				new_movie_data = fetch(str(all_database_movies[random_movie]), title_id)
			if last_movie == 'ne':
				random_movie = int(random.randint(0, len(all_database_movies)))
				return fetch(str(all_database_movies[random_movie]), title_id)
			print('** Line 52: {0}\t{1}\t{2}\t{3}'.format(last_movie, new_movie_data['id'], movie_db, type(last_movie)))
			print('** Line 53: {0}'.format(data_error.pgerror))
		# else:
		# print('Line 63: Query Executed')
		# print('{0} for {1}'.format(cur.query, last_movie))
		# print('Line 65: **************')
		# return {}  # urllib.request.urlopen(req).read().decode('utf-8')
		return new_movie_data
	except urllib.error.URLError as e:
		# Not an HTTP-specific error (e.g. connection refused)
		print('URLError: {1} Reason: {0}'.format(e.reason, e.strerror))
		return {}
	else:
		# 200
		data = remote_data.read().decode('utf-8')
		return json.loads(data)


def insert_language(languages):
	for language in languages:
		lang_q = "INSERT INTO movies.languages VALUES (%s,%s) ON CONFLICT (lang_code) do nothing"
		cur.execute(lang_q, [language['iso_639_1'], language['name']])
		conn.commit()


def overview():
	link = "https://baconipsum.com/api/?type=all-meat&paras=2&start-with-lorem=1"
	try:
		mozilla = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
		req = urllib.request.Request(
			link,
			data=None,
			headers={
				'User-Agent': mozilla
			}
		)
		remote_data = urllib.request.urlopen(req)
	except urllib.error.HTTPError as e:
		print('\n*****\nHTTPError: {0} ({1})\n*****\n'.format(e.code, e.msg))
		pass
	else:
		data = remote_data.read().decode('utf-8')
		bacon = json.loads(data)
		return bacon[0] + bacon[1]


def get_movie():
	last_query = "SELECT id FROM movies.tmdb"
	# last_query = "SELECT tmdb FROM movies.links WHERE tmdb NOT IN (SELECT id FROM movies.tmdb)"
	cur.execute(last_query, [])
	all_movies = cur.fetchall()
	for identity in range(len(all_movies)):
		# print('Identity {0} for {1}'.format(identity, all_movies[identity][0]))
		# identities[identity] = all_movies[identity][0]
		identities.append(all_movies[identity][0])
	return identities


# Number of Pages: ceil(45870/5000) = ceil(9.174) = 10
movies = 5000
page = 9
deep = 330
start = int(time.time())
have_done = 0
results = read_links()
identities = []
all_database_movies = get_movie()
# request_data_version_4 = {
# 	"request_token": "eyJhbGciOiJsUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE0NzIwNzYxMjAsInZlcnNpb24iOjEsImV4cCI6MTQ3MjA3NzAyMCwiYdRkIjoiM2Y4Nzg1N2JlMjA5ZDM1MTk4MzNiMzAwYTEzZDBlMTIiSqJzY29wZXMiOlsicGVuZGluZ19yZXF1ZXN0X3Rva2VuIl0sImp0USI6MTB9.Rt-xi8wfscw2_P09T4BuxxUJtF7XReqKODh2HW61FIY"
# }
for row in results:
	if row[2] is not None:
		movie_data = fetch(str(row[2]), row[0])

		if 'title' not in movie_data:
			movie_data['title'] = "Anonymous Movie"
			print('Line 126: {0} not found...'.format(movie_data['title']))

		# if 'overview' not in movie_data:
		# 	movie_data['overview'] = overview()
		# 	print("Overview for ", row[0], ":", movie_data['overview'])
		# movie_data['overview'] = "Overview of the Movie"

		if 'release_date' not in movie_data or len(movie_data['release_date']) == 0:
			year = random.randrange(1970, 2018)
			movie_data['release_date'] = '1970-01-01'
		else:
			(year, month, day) = movie_data['release_date'].split('-')

		if 'poster_path' not in movie_data or movie_data['poster_path'] is None:
			poster = None
		else:
			poster = "https://image.tmdb.org/t/p/w780" + movie_data['poster_path']

		if 'spoken_languages' in movie_data:
			insert_language(movie_data['spoken_languages'])

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
		try:
			cur.execute(movie_query, parameters)
			conn.commit()
		except psy.DataError as error:
			print('Line 194: Data Error Found: {0}:\t{1} {2}'.format(error.pgcode, error.pgerror, int(row[0])))
		except psy.IntegrityError as integrity:
			print('Line 196: Error {0} with code {1}'.format(integrity.pgerror, integrity.pgcode))
		except psy.InternalError as internal:
			print('Line 198: Internal Error\n** {0}'.format(internal.pgerror))
		else:
			per_cent = 100 * (float(page * movies + have_done + deep + 1) % 45870) / 45870
			if per_cent < 10:
				percent = "{:,.6f}".format(per_cent)
			else:
				percent = '{:,.5f}'.format(per_cent)
			have = str((have_done + deep) % movies).zfill(len(str(movies)))
			title = movie_data['title']
			try:
				last_query = "DELETE FROM movies.tmdb WHERE id = %s"
				cur.execute(last_query, [row[2]])
				# Commended για καλλωπιστικούς λόγους
				# print("Line 198:\t{0}".format(cur.query))
			except psy.DataError as error:
				print('Line 200:\t{0}'.format(error.pgerror))
			else:
				conn.commit()
				print(
					"[{0}/{1}]\t{2}%\tUpdated {3} as {4} ({5}) rated {6} from {7} users".format(have, movies, percent,
					                                                                            row[0], title,
					                                                                            year,
					                                                                            movie_data['vote_average'],
					                                                                            movie_data['vote_count']))
	have_done += 1

print("That whole thing lasted {} seconds for {} movies".format(int(time.time()) - start, movies))
print("These were movies #{} until #{}".format(page * movies, (page + 1) * movies))
