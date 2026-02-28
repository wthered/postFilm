import math

from credentials import *
from misc_functions import *
# Local Imports
from people_functions import *
from similarity_functions import *


def find_next_film(connection):
	connection.last_query = "SELECT nextval('movies_id_seq') AS next_movie"
	connection.select([], False, False)
	for movie in range(next_item.get('film', 4) - 3, connection.results.get('next_movie') + 3):
		connection.last_query = "SELECT * FROM movies WHERE id = %s"
		connection.select([
			movie
		], False, False)
		if not connection.results:
			next_item.update({
				'film': movie
			})
			with open("next_file.json", 'w') as jason:
				json.dump(next_item, jason, indent=4)
			# When you open a file using the with statement, Python automatically closes the file once the block inside the
			# with statement is exited, regardless of how the block is exited
			return movie
	return None


def find_next_company(connection):
	connection.last_query = "SELECT nextval('companies_id_seq') AS next_company"
	connection.select([
		connection_info.get('database')
	], False, False)
	for company in range(next_item.get('company', 4) - 3, connection.results.get('next_company') + 3):
		connection.last_query = "SELECT * FROM companies WHERE id = %s"
		connection.select([
			company
		], False, False)
		if connection.results is None:
			next_item.update(dict({
				'company': company
			}))
			with open("next_file.json", 'w') as jason:
				json.dump(next_item, jason, indent=4)
			# When you open a file using the with statement, Python automatically closes the file once the block inside the
			# with statement is exited, regardless of how the block is exited
			return company
	return None


def find_next_video(connection):
	connection.last_query = "SELECT nextval('videos_id_seq') AS next_video"
	connection.select([], False, False)
	for video in range(next_item.get('video', 4) - 3, connection.results.get('next_video') + 3):
		connection.last_query = "SELECT * FROM videos WHERE id = %s"
		connection.select([
			video
		], False, False)
		if connection.results is None:
			next_item.update(dict({
				'video': video
			}))
			with open("next_file.json", 'w') as jason:
				json.dump(next_item, jason, indent=4)
			# When you open a file using the with statement, Python automatically closes the file once the block inside the
			# with statement is exited, regardless of how the block is exited
			return video
	return None


def find_next_keyword(connection):
	connection.last_query = "SELECT nextval('keywords_id_seq') AS next_keyword"
	connection.select([
		connection_info.get('database')
	], False, False)
	for keyword in range(next_item.get('keyword', 4) - 3, connection.results.get('next_keyword') + 3):
		connection.last_query = "SELECT * FROM keywords WHERE id = %s"
		connection.select([
			keyword
		], False, False)
		if connection.results is None:
			next_item.update(dict({
				'keyword': keyword
			}))
			with open("next_file.json", 'w') as jason:
				json.dump(next_item, jason, indent=4)
			# When you open a file using the with statement, Python automatically closes the file once the block inside the
			# with statement is exited, regardless of how the block is exited
			return keyword
	return None


def get_film_info(link):
	response = requests.get('https://api.themoviedb.org/3/movie/{}?language=en-US&append_to_response=videos,keywords,credits'.format(link), headers=http_headers).json()
	if 'success' in response or response.get('adult', False):
		return None
	response.update({
		"imdb_id": fix_page(response.get("imdb_id")),
		"homepage": long_links[link] if link in long_links else response.get('homepage'),
		"released": fix_date(response.get("release_date"), "%Y-%m-%d"),
		"tagline": response.get('tagline') if len(response.get('tagline')) < 255 else "{}&hellip;".format(response.get('tagline')[:245]),
	})
	return response


def handle_movie(connection, film_info):
	if film_info.get('imdb_id') is None:
		connection.last_query = "SELECT * FROM links WHERE entry_type = %s AND link = %s AND page IS NULL"
		connection.select([
			"App\\Models\\Movie",
			film_info.get('id')
		], False, False)
	else:
		connection.last_query = "SELECT * FROM links WHERE entry_type = %s AND (link = %s OR page = %s)"
		connection.select([
			"App\\Models\\Movie",
			film_info.get('id'),
			film_info.get('imdb_id')
		], False, False)
	if connection.results is None and film_info.get('vote_count') > 1024:
		return create_movie(connection, film_info)
	if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
		return update_movie(connection, film_info, connection.results.get('entry_id'))
	return connection.results.get('entry_id')


def create_movie(connection, film_info):
	movie_id = find_next_film(connection)
	time_created = create_movie_time(film_info)
	current_time = datetime.now(time_zone).replace(hour=np.random.randint(10, 24), minute=np.random.randint(10, 60), second=np.random.randint(10, 60)).strftime(date_format)
	connection.last_query = "INSERT INTO movies (id, title, original_title, image, poster, plot, language, release_date, budget, revenue, duration, status, votes, rating, score, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s);"
	movie_data = [
		movie_id,
		film_info.get('title'),
		film_info.get('original_title'),
		film_info.get('backdrop_path'),
		film_info.get('poster_path'),
		film_info.get('overview'),
		film_info.get('original_language'),
		film_info.get('release_date') if film_info.get('release_date') else None if film_info.get('release_date') else None,
		film_info.get('budget'),
		abs(film_info.get('revenue')),
		film_info.get('runtime'),
		film_info.get('status'),
		film_info.get('vote_count'),
		film_info.get('vote_average'),
		film_info.get('popularity'),
		time_created if time_created.year > 1969 else current_time
	]
	connection.insert(movie_data, False, True)
	return create_link(connection, movie_id, film_info)


def create_link(connection, next_film, film_info):
	link_created = create_movie_time(film_info)
	connection.last_query = "INSERT INTO links (entry_type, entry_id, link, page, visited, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
	link_data = [
		"App\\Models\\Movie",
		next_film,
		film_info.get('id'),
		film_info.get('imdb_id'),
		False,
		link_created if link_created.year > 1969 else datetime.now(time_zone).strftime(date_format),
		datetime.now(time_zone).strftime(date_format)
	]
	connection.insert(link_data, False, True)
	return next_film


def update_movie(connection, film_info, this_film):
	connection.last_query = "UPDATE movies SET title = %s, original_title = %s, image = %s, poster = %s, plot = %s, language = %s, release_date = %s, budget = %s, revenue = %s, duration = %s, status = %s, votes = %s, rating = %s, score = %s, updated_at = NOW() WHERE id = %s"
	movie_data = [
		film_info.get('title'),
		film_info.get('original_title'),
		film_info.get('backdrop_path'),
		film_info.get('poster_path'),
		film_info.get('overview'),

		film_info.get('original_language'),
		film_info.get('release_date') if film_info.get('release_date') else None,
		film_info.get('budget'),
		film_info.get('revenue'),
		film_info.get('runtime'),
		film_info.get('status'),
		film_info.get('vote_count'),
		film_info.get('vote_average'),
		film_info.get('popularity'),
		this_film
	]
	if film_info.get('vote_count') > 1024:
		connection.update(movie_data, False, True)
	return update_link(connection, this_film, film_info)


def update_link(connection, this_film, film_info):
	connection.last_query = "UPDATE links SET link = %s, page = %s, updated_at = NOW() WHERE entry_type = %s AND entry_id = %s"
	connection.update([
		film_info.get('id'),
		film_info.get('imdb_id'),
		"App\\Models\\Movie",
		this_film
	], False, True)
	return this_film


def handle_companies(connection, companies, movie_id):
	for company in companies:
		connection.last_query = "SELECT * FROM companies WHERE link = %s"
		connection.select([
			company.get('id')
		], False, False)
		company_result = connection.results
		this_company = company_result.get('id') if company_result is not None else None
		if company_result is None:
			this_company = find_next_company(connection)
			connection.last_query = "INSERT INTO companies (id, name, logo, link, country_code, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())"
			company_options = [
				this_company,
				company.get('name'),
				company.get('logo_path'),
				company.get('id'),
				company.get('origin_country') if company.get('origin_country') else None
			]
			connection.insert(company_options, False, True)
		else:
			if company_result.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
				connection.last_query = "UPDATE companies SET name = %s, logo = %s, country_code = %s, updated_at = NOW() WHERE link = %s"
				company_options = [
					company.get('name'),
					company.get('logo_path'),
					company.get('origin_country') if company.get('origin_country') else None,
					company.get('id')
				]
				connection.update(company_options, False, True)
				this_company = company_result.get('id')
		handle_companies_movies(connection, this_company, movie_id)


def handle_companies_movies(connection, company_id, movie_id):
	entry_data = [
		company_id,
		movie_id
	]
	connection.last_query = "SELECT * FROM companies_movies WHERE company_id = %s AND movie_id = %s"
	connection.select(entry_data, False, False)
	if connection.results is None:
		connection.last_query = "INSERT INTO companies_movies (company_id, movie_id, created_at, updated_at) VALUES (%s, %s, NOW(), NOW())"
		connection.insert(entry_data, False, True)
	else:
		if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
			connection.last_query = "UPDATE companies_movies SET updated_at = NOW() WHERE company_id = %s and movie_id = %s"
			connection.update(entry_data, False, True)


def handle_countries(connection, countries, movie_id):
	for country in countries:
		connection.last_query = "SELECT * FROM countries WHERE iso_code = %s"
		connection.select([
			country.get('iso_3166_1')
		], True, False)
		founded_country = connection.results.get('code') if connection.results is not None else None
		if connection.results is not None:
			# Check if the updated_at time is older than 8 days
			if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
				founded_country = update_country(connection, country)
		else:
			founded_country = create_country(connection, country)
		handle_countries_movies(connection, founded_country, movie_id)


def fetch_country_info(connection, country_code):
	response = [
		{}
	]
	country_url = "https://restcountries.com/v3.1/alpha/{}".format(country_code)
	try:
		response = requests.get(country_url, headers=http_headers).json()
	except requests.exceptions.JSONDecodeError:
		print("\nJSON Decode Error for {}".format(country_url))
		connection.last_query = "SELECT * FROM countries WHERE code = %s"
		connection.select([
			country_code
		], False, False)
		if connection.results is None:
			sys.exit(1)
		else:
			return connection.results
	except requests.exceptions.SSLError:
		return None

	try:
		return response[0] if response else None
	except KeyError:
		print("\nKeyError for {}".format(country_url))
		connection.last_query = "SELECT * FROM countries WHERE code = %s"
		connection.select([
			country_code
		], False, False)
		if connection.results is None:
			sys.exit(1)
		return dict({
			'region': None,
			'subregion': None,
			'area': 0,
			'population': 0,
			'landlocked': False
		})


def create_country(connection, country_data):
	remote_country = fetch_country_info(connection, country_data.get('iso_3166_1'))
	print(remote_country)
	connection.last_query = "INSERT INTO countries (code, name, iso_code, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())"
	country_options = [
		country_data.get('iso_3166_1'),
		country_data.get('name'),
		remote_country.get('cca2')
	]
	connection.insert(country_options, True, True)
	return country_data.get('iso_3166_1')


def update_country(connection, country_data):
	# remote_country = fetch_country_info(connection, country_data.get('iso_3166_1'))
	connection.last_query = "UPDATE countries SET name = %s, updated_at = NOW() WHERE code = %s"
	connection.update([
		country_data.get('name'),
		country_data.get('iso_3166_1')
	], False, True)
	return country_data.get('iso_3166_1')


def handle_countries_movies(connection, country_code, movie_id):
	connection.last_query = "SELECT * FROM entries_countries WHERE country_code = %s AND entry_type = %s AND entry_id = %s"
	connection.select([
		country_code,
		"App\\Models\\Movie",
		movie_id
	], False, False)
	if connection.results is None:
		connection.last_query = "INSERT INTO entries_countries (entry_type, entry_id, country_code, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())"
		connection.insert([
			"App\\Models\\Movie",
			movie_id,
			country_code
		], False, True)
	else:
		if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
			connection.last_query = "UPDATE entries_countries SET updated_at = NOW() WHERE country_code = %s AND entry_type = %s AND entry_id = %s"
			connection.update([
				country_code,
				"App\\Models\\Movie",
				movie_id
			], False, True)


def handle_languages(connection, languages, entry_type, entry_id):
	for language in languages:
		connection.last_query = "SELECT * FROM languages WHERE code = %s"
		connection.select([
			language.get('iso_639_1')
		], False, False)
		if connection.results is None:
			connection.last_query = "INSERT INTO languages (code, name, english_name, movies_count, series_count, created_at, updated_at) VALUES (%s, %s, %s, 0, 0, NOW(), NOW())"
			connection.insert([
				language.get('iso_639_1'),
				language.get('name'),
				language.get('english_name')
			], False, True)
		else:
			if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
				connection.last_query = "UPDATE languages SET updated_at = NOW() WHERE code = %s"
				connection.update([
					language.get('iso_639_1')
				], False, True)
		handle_languages_entries(connection, language, entry_type, entry_id)


def handle_languages_entries(connection, language, entry_type, entry_id):
	connection.last_query = "SELECT * FROM entries_languages WHERE language_code = %s AND entry_type = %s AND entry_id = %s"
	connection.select([
		language.get('iso_639_1'),
		entry_type,
		entry_id
	], False, False)
	if connection.results is None:
		connection.last_query = "INSERT INTO entries_languages (language_code, entry_type, entry_id, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())"
		connection.insert([
			language.get('iso_639_1'),
			entry_type,
			entry_id
		], False, True)
	else:
		connection.last_query = "UPDATE entries_languages SET updated_at = NOW() WHERE language_code = %s AND entry_type = %s AND entry_id = %s"
		connection.update([
			language.get('iso_639_1'),
			entry_type,
			entry_id
		], False, True)
	connection.last_query = "UPDATE languages SET {} = (SELECT COUNT(*) FROM entries_languages WHERE language_code = %s AND entry_type = %s) WHERE code = %s".format('movies_count' if entry_type == 'App\\Models\\Movie' else 'series_count')
	connection.update([
		language.get('iso_639_1'),
		entry_type,
		language.get('iso_639_1')
	], False, True)


def handle_videos(connection, videos, movie_id):
	if videos is None:
		print("None videos found for {}".format(movie_id))
	for video in videos.get('results', [
		{}
	]):
		if video.get('site') == "YouTube":
			connection.last_query = "SELECT * FROM videos WHERE link = %s"
			connection.select([
				video.get('key')
			], False, False)
			video_line = connection.results
			try:
				video_date = datetime.strptime(video.get('published_at'), "%Y-%m-%dT%H:%M:%S.%fZ").strftime(date_format)
			except ValueError:
				video_date = fix_date(video.get('published_at'), "%Y-%m-%d %H:%M:%S %Z")
			if video_line is None:
				# print("[{}@{} - Film #{}] Create Video `{}`{}".format(datetime.now().strftime(date_format), time_zone.zone, movie_id, video.get('name'), '.' * 4), end='\r')
				this_video = find_next_video(connection)
				connection.last_query = "INSERT INTO videos (id, name, link, movie_id, size, type, is_official, token, created_at, updated_at, published_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)"
				video_data = [
					this_video,
					video.get('name'),
					video.get('key'),
					movie_id,
					video.get('size'),
					video.get('type'),
					video.get('official'),
					video.get('id'),
					video_date
				]
				connection.insert(video_data, False, True)
			else:
				# print("[{}@{} - Film #{}] Update Video `{}`{}".format(datetime.now().strftime(date_format), time_zone.zone, movie_id, video.get('name'), '.' * 4), end='\r')
				connection.last_query = "UPDATE videos SET name = %s, size = %s, type = %s, token = %s, updated_at = NOW(), published_at = %s WHERE link = %s"
				video_data = [
					video.get('name'),
					video.get('size'),
					video.get('type'),
					video.get('id'),
					video_date,
					video.get('key')
				]
				connection.update(video_data, False, True)


def handle_words(connection, keywords, film_id):
	for keyword in keywords.get('keywords'):
		connection.last_query = "SELECT * FROM keywords WHERE word = %s"
		connection.select([
			keyword.get('name')
		], False, False)
		word_id = connection.results.get('id') if connection.results is not None else None
		if connection.results is None:
			word_id = create_keyword(connection, keyword)
		else:
			if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
				update_keyword(connection, keyword)
		handle_keywords_movies(connection, word_id, film_id)


def create_keyword(connection, keyword):
	keyword_id = find_next_keyword(connection)
	connection.last_query = "INSERT INTO keywords (id, word, movies_count, series_count, created_at, updated_at) VALUES (%s, %s, 1, %s, NOW(), NOW())"
	connection.insert([
		keyword_id,
		keyword.get('name'),
		None
	], False, True)
	return keyword_id


def update_keyword(connection, keyword):
	connection.last_query = "UPDATE keywords SET updated_at = NOW() WHERE word = %s"
	connection.update([
		keyword.get('name')
	], False, True)


def handle_keywords_movies(connection, keyword, movie):
	connection.last_query = "SELECT * FROM entries_keywords WHERE keyword_id = %s AND entry_type = %s AND entry_id = %s"
	connection.select([
		keyword,
		"App\\Models\\Movie",
		movie
	], False, False)
	entry_results = connection.results
	if entry_results is None:
		connection.last_query = "INSERT INTO entries_keywords (keyword_id, entry_type, entry_id, movies_count, series_count, created_at, updated_at) VALUES (%s, %s, %s, 0, 0, NOW(), NOW())"
		connection.insert([
			keyword,
			"App\\Models\\Movie",
			movie
		], False, True)
	else:
		if entry_results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
			connection.last_query = "UPDATE entries_keywords SET updated_at = NOW() WHERE keyword_id = %s AND entry_type = %s AND entry_id = %s"
			connection.update([
				keyword,
				"App\\Models\\Movie",
				movie
			], False, True)
	connection.last_query = "UPDATE keywords SET movies_count = (SELECT COUNT(*) FROM entries_keywords WHERE entry_type = %s AND keyword_id = %s) WHERE id = %s"
	connection.update([
		"App\\Models\\Movie",
		keyword,
		keyword
	], False, True)


def handle_genres(db, genres, film):
	for genre in genres:
		db.last_query = "SELECT * FROM genres WHERE name = %s"
		db.select([
			genre.get('name')
		], False, False)
		this_moment = datetime.now(time_zone).strftime(date_format)
		if db.results is None:
			db.last_query = "INSERT INTO genres (name, movies_count, series_count, created_at, updated_at) VALUES (%s, 1, 0, %s, %s)"
			db.insert([
				genre.get('name'),
				this_moment,
				this_moment
			], False, True)
		else:
			if db.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
				db.last_query = "UPDATE genres SET updated_at = NOW() WHERE name = %s"
				db.update([
					genre.get('name')
				], False, True)
		handle_genre_movies(db, genre.get('name'), film)


def handle_genre_movies(database, genre_name, movie):
	database.last_query = "SELECT * FROM entries_genres WHERE entry_type = %s AND entry_id = %s AND genre_id IN (SELECT id FROM genres WHERE name = %s)"
	database.select([
		'App\\Models\\Movie',
		movie,
		genre_name
	], False, False)
	if database.results is None:
		database.last_query = "INSERT INTO entries_genres (genre_id, entry_type, entry_id, created_at, updated_at) SELECT id, %s, %s, NOW(), NOW() FROM genres WHERE name = %s"
		database.insert([
			'App\\Models\\Movie',
			movie,
			genre_name
		], False, True)
	else:
		if database.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
			database.last_query = "UPDATE entries_genres SET updated_at = NOW() WHERE entry_type = %s AND entry_id = %s AND genre_id IN (SELECT id FROM genres WHERE name = %s)"
			database.update([
				'App\\Models\\Movie',
				movie,
				genre_name
			], False, True)
	database.last_query = "UPDATE genres SET movies_count = (SELECT COUNT(*) FROM entries_genres WHERE entry_type = %s AND genre_id IN (SELECT id FROM genres WHERE name = %s)) WHERE name = %s"
	database.update([
		'App\\Models\\Movie',
		genre_name,
		genre_name
	], False, True)


def process(connection, film_info):
	if film_info.get('vote_count') < 1024 or film_info.get('vote_average') < 1:
		return None
	film_item = dict({
		"film": handle_movie(connection, film_info),
		"page": film_info.get("imdb_id"),
		"link": film_info.get('id')
	})
	# print("[Movie Functions:420] {}".format(film_item))
	# print("[{}@{}] Handling people for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_people(connection, film_info.get('credits', {
		'cast': [
			{}
		],
		'crew': [
			{}
		]
	}), film_item.get('film'))
	# print("[{}@{}] Handling genres for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_genres(connection, film_info.get('genres'), film_item.get('film'))
	# print("[{}@{}] Handling countries for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_countries(connection, film_info.get('production_countries'), film_item.get('film'))
	# print("[{}@{}] Handling companies for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_companies(connection, film_info.get('production_companies'), film_item.get('film'))
	# print("[{}@{}] Handling languages for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_languages(connection, film_info.get('spoken_languages'), "App\\Models\\Movie", film_item.get('film'))
	# print("[{}@{}] Handling videos for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_videos(connection, film_info.get('videos', {
		"results": []
	}), film_item.get('film'))
	# print("[{}@{}] Handling words for `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, film_info.get('title'), end='{}\r'.format('.' * dots)))
	handle_words(connection, film_info.get('keywords', {
		"keywords": []
	}), film_item.get('film'))

	last_update = (datetime.now(time_zone) - timedelta(days=days)).strftime(date_format)
	# Delete old entries with same ID (ID could be saved but movie updated)
	connection.last_query = "DELETE FROM entries_languages WHERE entry_type = %s AND entry_id = %s AND updated_at < %s"
	connection.delete([
		"App\\Models\\Movie",
		film_item.get('film'),
		last_update
	], False, True)
	connection.last_query = "DELETE FROM entries_keywords WHERE entry_type = %s AND entry_id = %s AND updated_at < %s"
	connection.delete([
		"App\\Models\\Movie",
		film_item.get('film'),
		last_update
	], False, True)
	connection.last_query = "DELETE FROM entries_genres WHERE entry_type = %s AND entry_id = %s AND updated_at < %s"
	connection.delete([
		"App\\Models\\Movie",
		film_item.get('film'),
		last_update
	], False, True)
	connection.last_query = "DELETE FROM companies_movies WHERE movie_id = %s AND updated_at < %s"
	connection.delete([
		film_item.get('film'),
		last_update
	], False, True)
	connection.last_query = "DELETE FROM people_roles WHERE entry_type = %s AND entry_id = %s AND updated_at < %s"
	connection.delete([
		"App\\Models\\Movie",
		film_item.get('film'),
		last_update
	], False, True)
	connection.last_query = "DELETE FROM entries_countries WHERE entry_id = %s AND updated_at < %s"
	connection.delete([
		film_item.get('film'),
		last_update
	], False, True)
	return film_item.get('film')


def get_similar(connection, source_info, movie_frame):
	# print("\n[{}@{}] Movie Frame:\n{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, movie_frame))
	result_frame = movie_frame.copy()
	try:
		source_item = dict({
			'film': movie_frame.at[source_info.get('id'), 'film'].item(),
			'page': movie_frame.at[source_info.get('id'), 'page'].item(),
			'link': source_info.get('id')
		})
	except AttributeError:
		source_item = dict({
			'film': movie_frame.at[source_info.get('id'), 'film'],
			'page': movie_frame.at[source_info.get('id'), 'page'],
			'link': source_info.get('id')
		})
	page = 1
	url = "https://api.themoviedb.org/3/movie/{}/similar?language=en-US&page={}".format(source_info.get('id'), page)
	response = requests.get(url, headers=http_headers).json()
	# pages = min(16, response.get('total_pages')) #
	pages = min(math.ceil(datetime.now(time_zone).second / 2) + 3, response.get('total_pages'))
	start_time = datetime.now(time_zone)
	start_size = movie_frame.shape[0]
	while page <= pages:
		url = "https://api.themoviedb.org/3/movie/{}/similar?language=en-US&page={}".format(source_info.get('id'), page)
		# print("[Page {} / {}] Requesting {}".format(page, pages, url))
		response = requests.get(url, headers=http_headers).json()
		if response.get('total_results') == 0:
			return movie_frame
		for result in response.get('results'):
			frame_size = movie_frame.shape[0]
			target_info = get_film_info(result.get('id'))
			if target_info is None:
				continue
			if source_item.get('page') is None:
				connection.last_query = "SELECT * FROM links WHERE entry_type = %s AND link = %s AND page IS NULL"
				connection.select([
					'App\\Models\\Movie',
					result.get('id')
				], False, False)
			else:
				connection.last_query = "SELECT * FROM links WHERE entry_type = %s AND link = %s OR page = %s"
				connection.select([
					'App\\Models\\Movie',
					result.get('id'),
					target_info.get('imdb_id')
				], False, False)
			if connection.results is None:
				target_item = dict({
					'film': process(connection, target_info),
					'page': target_info.get('imdb_id'),
					'link': target_info.get('id')
				})
				recent_entry = True
			else:
				target_item = dict({
					'film': connection.results.get('entry_id'),
					'page': target_info.get('imdb_id'),
					'link': target_info.get('id')
				})
				print("[{}@{} - Page {} / {}] Skipped {}....".format(when_frame_ends(start_time, start_size, movie_frame), time_zone.zone, str(page).rjust(2, " "), pages, target_item), end='\r')
				continue
			similarity = calculate_similarity(connection, source_item, source_info, target_item, target_info)
			if recent_entry:
				result_frame.at[target_item.get('link'), 'film'] = target_item.get('film')
				result_frame.at[target_item.get('link'), 'page'] = target_info.get('imdb_id')
				result_frame.at[target_item.get('link'), 'title'] = target_info.get('title')
				result_frame.at[target_item.get('link'), 'rating'] = target_info.get('vote_average')
				result_frame.at[target_item.get('link'), 'votes'] = target_info.get('vote_count')
				result_frame.at[target_item.get('link'), 'score'] = target_info.get('popularity')
				# Remove .date() from pd.to_datetime(target_info.get('released')) to keep the value in datetime64[ns] format
				result_frame.at[target_item.get('link'), 'released'] = pd.to_datetime(target_info.get('released')) if target_info.get('released') else None
				result_frame.at[target_item.get('link'), 'similarity'] = similarity
				result_frame.at[target_item.get('link'), 'updated_at'] = datetime.now(time_zone).strftime(date_format)

			left_rounds = "{} left".format(modulus - result_frame.shape[0] % modulus) if result_frame.shape[0] % modulus > 0 else "{} rounds".format(int(result_frame.shape[0] / modulus))
			print("[{}@{} - Page {} / {}] Frame has {} movies ({}) after `{}`{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, str(page).rjust(2, " "), pages, result_frame.shape[0], left_rounds, result.get('title'), '.' * 8),
				end='\r' if frame_size == result_frame.shape[0] else '\n')
			if result_frame.shape[0] % modulus == 0 and recent_entry:
				# movie_frame = movie_frame[['film', 'page', 'title', 'rating', 'votes', 'similarity', 'updated_at']]
				result_frame.sort_index(inplace=True)
				print("\n[{}@{}] These movies will be processed\n{}".format(when_frame_ends(start_time, start_size, movie_frame), time_zone.zone, movie_frame))
				connection.last_query = "DELETE FROM similarities WHERE similarity_score = %s"
				connection.delete([
					0
				], True, True)
			time.sleep(1)
		result_frame.sort_index(inplace=True)
		result_frame.to_csv('movie_file.csv')
		page += 1
		time.sleep(1)
	return result_frame


def get_filmography(connection, film_person, entry_type, data_frame):
	if data_frame.empty:
		data_frame = pd.DataFrame([
			{
				'film': None,
				'link': 1,
				'title': 'Point Of No Return',
				'rating': 0.0,
				'updated_at': datetime.now(time_zone).strftime(date_format),
				'adult': False
			}
		]).set_index('link')
	dots = 2
	url = "https://api.themoviedb.org/3/person/{}/movie_credits?language=en-US".format(film_person.get('id'))
	response = requests.get(url, headers=http_headers).json()
	if entry_type not in response:
		return data_frame
	person_entry = []
	for movie in response.get(entry_type):
		if movie.get('adult', False):
			dots = max(dots, len(movie.get('title')))
			print("[{}@{}] Skipping {}{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, movie.get('title'), '.' * dots), end='\r')
			continue
		result_info = get_film_info(movie.get('id'))
		if result_info is not None:
			person_entry.append((result_info.get('id'), result_info.get('imdb_id')))
	connection.last_query = generate_sql_query(person_entry)
	connection.select([
		'App\\Models\\Movie',
		film_person.get('id')
	], False, True)
	if not connection.results:
		return data_frame
	result_frame = pd.DataFrame(connection.results).set_index('link')
	while not result_frame.empty:
		for _ in result_frame.index:
			result_index = np.random.choice(result_frame.shape[0])
			result_movie = result_frame.index[result_index]
			result_info = get_film_info(result_movie)
			if result_info is None:
				continue
			film_item = dict({
				'film': process(connection, result_info),
				'link': result_info.get('id'),
				'page': result_info.get('page')
			})
			release_date = pd.to_datetime(result_info.get('release_date'), errors='coerce').strftime("%Y-%m-%d") if result_info.get('release_date') else pd.NaT
			print("[{}@{} - {} / {}] {} got a part in `{}` or {} released at {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, result_index + 1, result_frame.shape[0], film_person.get('name'), result_info.get('title'), film_item,
				release_date))
			result_frame.drop(result_movie, inplace=True)
			if result_frame.empty:
				break
		# Create empty dataframe so that we exit the infinite while loop
		result_frame = pd.DataFrame()
	return data_frame


def generate_sql_query(tuple_list):
	"""
	Generates a safe SQL query string from a list of (link, page) tuples,
	correctly handling NULL values for the `page` column.

	Args:
		tuple_list (list): A list of (link, page) tuples where 'page' can be None.

	Returns:
		str: The generated SQL query string.
	"""

	# 1. Separate the tuples into two lists based on the 'page' value
	not_in_values = []
	is_null_conditions = []

	for link, page in tuple_list:
		if page is None:
			# If `page` is None, we need to handle it with an `IS NULL` check.
			is_null_conditions.append(f"(link = {link} AND page IS NULL)")
		else:
			# Otherwise, it can be safely used in a `NOT IN` clause.
			not_in_values.append(f"({link}, {page})")

	# 2. Build the `NOT IN` part of the query
	not_in_clause = ""
	if not_in_values:
		not_in_clause = f"AND (link, page) NOT IN ({', '.join(not_in_values)})"

	# 3. Build the `IS NULL` part of the query
	is_null_clause = ""
	if is_null_conditions:
		is_null_clause = f"AND NOT ({' OR '.join(is_null_conditions)})"

	# 4. Combine all the parts into a final query string
	base_query = "SELECT l.link, m.id AS film, l.page, m.title, m.score, m.votes, m.rating, m.release_date, m.updated_at, NULL AS similarity FROM movies AS m INNER JOIN people_roles AS person_role ON m.id = person_role.entry_id INNER JOIN links l ON l.entry_id = m.id INNER JOIN people ON people.id = person_role.person_id WHERE person_role.entry_type = %s AND l.entry_type = person_role.entry_type AND people.link = %s"

	return f"{base_query} {not_in_clause} {is_null_clause}"
