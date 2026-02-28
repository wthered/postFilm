import requests
from credentials import *

def get_daily_series(page=1):
	url = "https://api.themoviedb.org/3/trending/tv/week?language=el-GR&page={}".format(page)
	response = requests.get(url, headers=http_headers)
	return response.json()


def get_series_info(series_link):
	url = "https://api.themoviedb.org/3/tv/{}?language=el-GR".format(series_link)
	response = requests.get(url, headers=http_headers)
	if response.status_code == 404:
		return None
	return response.json()


def find_next_series(connection):
	connection.last_query = "SELECT nextval(pg_get_serial_sequence('series', 'id'))"
	connection.select([], False, False)
	next_show = connection.results.get('nextval') if connection.results else 1
	default_item = {"show": 5, "season": 5, "episode": 5}
	for tv_show in range(next_item.get('tv', default_item).get('show') - 3 if next_item.get('tv', default_item).get('show') > 3 else 1, next_show + 3):
		connection.last_query = "SELECT * FROM series WHERE id = %s"
		connection.select([tv_show], False, False)
		# print("Find next TV Show results: {}".format(connection.results))
		if connection.results is None:
			default_item.update({"show": tv_show})
			next_item.update({'tv': default_item})
			# print("[Television Functions] {}".format(next_item))
			with open("next_file.json", "w") as next_file:
				json.dump(next_item, next_file, indent=4)
			return tv_show


def find_next_season(database):
	return 1

def tv_genres(db, series_info, series_item):
	for genre in series_info.get('genres'):
		for result in [part.strip() for part in genre.get('name').split('-')]: # ['Action', 'Adventure']
			# print("[Line 39] Result: {}".format(result))
			db.last_query = "SELECT * FROM entries_genres WHERE entry_type = %s AND entry_id = %s"
			db.select(['App\\Models\\Series', series_item.get('link')], False, False)
			if db.results is None:
				db.last_query = "INSERT INTO entries_genres (entry_type, entry_id, genre_id, created_at, updated_at) SELECT %s, %s, id, NOW(), NOW() FROM genres WHERE name = %s"
				db.insert(['App\\Models\\Series', series_item.get('entry'), result], False, True)
			else:
				db.last_query = "UPDATE entries_genres SET updated_at = NOW() WHERE entry_type = %s, entry_id = %s, genre_id IN (SELECT id FROM genres WHERE name = %s)"
				db.update(['App\\Models\\Series', series_item.get('entry'), result], False, True)


def handle_series(db, info):
	db.last_query = "SELECT * FROM series WHERE link = %s"
	db.select([info.get('id')], False, False)
	if db.results is None:
		return create_series(db, info)
	return update_series(db, info, db.results.get('id'))

def count_series_seasons(info):
	number = 0
	last_episode_season = 0
	for season in info.get('seasons'):
		if season.get('season_number') == 0:
			continue
		number += 1
	last_episode = info.get('last_episode_to_air')
	print("Number os Seasons: {} or {}".format(number, last_episode.get('season_number')))
	return number


def create_series(db, info):
	this_series_id = find_next_series(db)
	db.last_query = "INSERT INTO series (id, title, first_air_date, last_air_date, description, rating, votes, cover_image, backdrop_image, seasons_count, episodes_count, homepage, link, in_production, score, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
	series_data = [
		this_series_id, info.get('name'), info.get('first_air_date'), info.get('last_air_date'), info.get('overview'), info.get('vote_average'), info.get('vote_count'), info.get('poster_path'),
		info.get('backdrop_path'), info.get('number_of_seasons'), info.get('number_of_episodes'), info.get('homepage'), info.get('id'), info.get('in_production'), info.get('popularity')
	]
	# print("Series Data: {}".format(series_data))
	db.insert(series_data, False, True)
	return this_series_id


def update_series(db, info, local_id):
	db.last_query = "UPDATE series SET first_air_date = %s, last_air_date = %s, description = %s, rating = %s, votes = %s, cover_image = %s, backdrop_image = %s, seasons_count = %s, episodes_count = %s, homepage = %s, in_production = %s, score = %s, updated_at = NOW() WHERE id = %s"
	series_data = [
		info.get('first_air_date'), info.get('last_air_date'), info.get('overview'), info.get('vote_average'), info.get('vote_count'), info.get('poster_path'),
		info.get('backdrop_path'), info.get('number_of_seasons'), info.get('number_of_episodes'), info.get('homepage'), info.get('in_production'), info.get('popularity'), local_id
	]
	db.update(series_data, False, True)
	return local_id


def handle_season(db, series_info, season, series_item):
	url = "https://api.themoviedb.org/3/tv/{}/season/{}?language=el-GR".format(series_info.get('id'), season)
	response = requests.get(url, headers=http_headers).json()
	print("Requested: {}".format(url))
	db.last_query = "SELECT * FROM seasons WHERE series_id = %s AND season_number = %s"
	db.select([series_item.get('entry'), tv_season], True, False)
	if db.results is None:
		db.last_query = "INSERT INTO seasons (id, series_id, season_number, title, description, air_date, rating, cover_image, link, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()"
		seasonal_data = [
			find_next_season(db), series_item.get('entry'), tv_season, response.get('name'), response.get('overview'),
			response.get('air_date'), response.get('vote_average'), response.get('poster_path'), response.get('id')
		]
		db.insert(seasonal_data, True, False)
		


def process_tv(db, series_info):
	series_item = dict({'entry': handle_series(db, series_info), 'link': series_info.get('id')})
	print("Series Item: {}".format(series_item))
	tv_genres(db, series_info, series_item)
	for tv_season in range(1, series_info.get('number_of_seasons')):
		season_details = handle_season(db, series_info, tv_season, series_item)
		