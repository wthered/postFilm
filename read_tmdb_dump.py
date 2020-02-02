import json
import time
import numpy as np
import requests
from database_connect import *
from datetime import datetime


def fetch(film_id):
	link = "https://api.themoviedb.org/3/movie/{}?api_key=1cd7d832933a3f8cb0c956ac70964e5f&append_to_response=videos".format(film_id)
	response = requests.get(link, headers=http_headers, proxies=proxy_servers)
	return response.json()


def get_next_film():
	db.query = "SELECT * FROM nextval('movies.titles_film_id_seq')"
	db.select([], True, False)
	next_film_id = db.results[0]
	for next_film in range(1, next_film_id + 5):
		db.query = "SELECT * FROM movies.titles WHERE film_id = %s"
		db.select([next_film], True, False)
		if db.results is None:
			return next_film


def insert_film_database():
	next_film_id = get_next_film()
	film.update(dict({'year': datetime.strptime(film_info['release_date'], "%Y-%m-%d").year}))
	film.update(dict({'date': datetime.strptime(film_info['release_date'], "%Y-%m-%d").date()}))
	film.update(dict({'this_time': round(time.time()), 'last_time': round(np.random.random() * time.time())}))

	if film_info['runtime'] is None:
		film_info['runtime'] = 0

	if film_info['poster_path'] is None:
		film.update(dict({'poster': None}))
	else:
		film.update(dict({'poster': 'http://image.tmdb.org/t/p/original{}'.format(film_info['poster_path'])}))

	if film_info['backdrop_path'] is None:
		film.update(dict({'backdrop': None}))
	else:
		film.update(dict({'backdrop': 'http://image.tmdb.org/t/p/original{}'.format(film_info['backdrop_path'])}))

	db.query = "SELECT film_id,title,year,rating,votes,duration,updated FROM movies.titles WHERE title = %s AND year = %s"
	db.select([film_info['title'], film['year']], True, True)
	try:
		that_film_id = db.results[0]
	except TypeError:
		that_film_id = None
	if db.results is None:
		db.query = "INSERT INTO movies.titles (film_id, title, year, rating, votes, original_title, original_lang, plot, poster, backdrop, score, released, revenue, budget, duration, status, tagline, updated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		film_data = [
			next_film_id, film_info['title'], film['year'], film_info['vote_average'], film_info['vote_count'], film_info['original_title'],
			film_info['original_language'], film_info['overview'], film['poster'], film['backdrop'], film_info['popularity'], film['date'],
			film_info['revenue'], film_info['budget'], film_info['runtime'], film_info['status'], film_info['tagline'], film['last_time']
		]
		db.insert(film_data, True, True)
		return next_film_id
	else:
		db.query = "UPDATE movies.titles SET title = %s, year = %s, rating = %s, votes = %s, original_title = %s, original_lang = %s, plot = %s, poster = %s, backdrop = %s, score = %s, released = %s, revenue = %s, budget = %s, duration = %s, status = %s, tagline = %s, updated = %s WHERE film_id = %s"
		film_data = [
			film_info['title'], film['year'], film_info['vote_average'], film_info['vote_count'], film_info['original_title'], film_info['original_language'],
			film_info['overview'], film['poster'], film['backdrop'], film_info['popularity'], film['date'], film_info['revenue'],
			film_info['budget'], film_info['runtime'], film_info['status'], film_info['tagline'], film['last_time'], that_film_id
		]
		db.update(film_data, True, True)
		return that_film_id


def get_film_trailer():
	film.update(dict({'trailer': None}))
	for video in film_info['videos']['results']:
		if video['site'] == "YouTube" and video['type'] == "Trailer":
			film.update(dict({'trailer': video['key']}))


def insert_link_database():
	db.query = "SELECT * FROM movies.links WHERE film_id = %s"
	db.select([new_film_id], True, True)
	film_links = db.results
	try:
		imdb_id = str(film_info['imdb_id'][2:]).lstrip("0")
		film.update(dict({'tmdb': film_info['id'], 'imdb': int(imdb_id)}))
	except ValueError:
		film.update(dict({'tmdb': film_info['id'], 'imdb': None}))

	get_film_trailer()

	if film_links is None:
		db.query = "INSERT INTO movies.links (film_id, film_tmdb, film_imdb, film_trailer, updated) VALUES (%s,%s,%s,%s,%s)"
		db.insert([new_film_id, film['tmdb'], film['imdb'], film['trailer'], film['last_time']], False, True)
	else:
		db.query = "UPDATE movies.links SET film_tmdb = %s, film_imdb = %s, film_trailer = %s, updated = %s WHERE film_id = %s"
		db.update([film['tmdb'], film['imdb'], film['trailer'], film['last_time'], new_film_id], False, True)


if __name__ == '__main__':
	db = Database()
	movies = dict()
	film = dict()
	with open("movie_ids_01_28_2020.json", "r", encoding='utf-8') as filepath:
		for line in filepath:
			film_line = json.loads(line)
			if not film_line['adult']:
				film_info = dict(fetch(film_line['id']))
				new_film_id = insert_film_database()
				insert_link_database()
			seconds = abs(round(np.random.normal(12, 4)))
			print("Sleeping {} seconds{}".format(seconds, '.' * (2 + seconds)))
			time.sleep(seconds)
