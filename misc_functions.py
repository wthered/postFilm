# Description: This file contains the functions that are used in the main file to handle the data from the API.
import requests
from movie_functions import *
import time


def fix_page(page):
	if page is None or not page:
		return None
	return int(str(page).strip('tt').strip('nn'))


def get_info(link):
	url = "https://api.themoviedb.org/3/movie/{}?language=el-GR&append_to_response=videos,keywords,credits".format(link)
	response = requests.get(url, headers=http_headers).json()
	response['imdb_id'] = fix_page(response.get('imdb_id'))
	if response.get('success', True) is False:
		return None
	return response


def find_next_company(database):
	database.last_query = "SELECT nextval('companies_id_seq')"
	database.select([], False, False)
	next_company = database.results.get('nextval') if database.results else 4
	# print("\nNext Company from {} till {}".format(next_item.get('company', 4) - 3, next_company))
	for company in range(next_item.get('company', 4) - 3 if next_item.get('company', 5) > 3 else 1, next_company + 3):
		database.last_query = "SELECT * FROM companies WHERE id = %s"
		database.select([
			company
		], False, False)
		if database.results is None:
			next_item.update({
				'company': company
			})
			with open("next_file.json", "w") as next_object_file:
				json.dump(next_item, next_object_file, indent=4)
			return company
	return None


def handle_companies(database, companies, film):
	for company in companies:
		if company.get('origin_country') == '':
			company['origin_country'] = None
		database.last_query = "SELECT * FROM companies WHERE link = %s"
		database.select([
			company.get('id')
		], False, False)
		if database.results is None:
			company_id = find_next_company(database)
			company_data = [
				find_next_company(database),
				company.get('name'),
				company.get('origin_country') if company.get('origin_country') else None,
				company.get('logo_path'),
				company.get('id')
			]
			database.last_query = "INSERT INTO companies (id, name, country_code, logo, link, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())"
			database.insert(company_data, False, True)  # print("[{}@{}] Company created: {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, company.get('name')))
		else:
			company_id = database.results.get('id')
			company_data = [
				company.get('name'),
				company.get('origin_country'),
				company.get('logo_path'),
				company_id
			]
			movie_time = pytz.UTC.localize(database.results.get('updated_at')).astimezone(time_zone)
			if movie_time > datetime.now(time_zone) - timedelta(days=days):
				continue
			database.last_query = "UPDATE companies SET name = %s, country_code = %s, logo = %s, updated_at = NOW() WHERE id = %s"
			database.update(company_data, False, True)  # print("[{}@{}] Company updated: {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, company.get('name')))
		handle_companies_movies(database, company_id, film)


def handle_companies_movies(database, company_id, film):
	# print("Company #{} for film: {}".format(company_id, film))
	database.last_query = "SELECT * FROM companies_movies WHERE company_id = %s AND movie_id = %s"
	database.select([
		company_id,
		film
	], False, False)
	if database.results is None:
		database.last_query = "INSERT INTO companies_movies (company_id, movie_id, created_at, updated_at) VALUES (%s, %s, NOW(), NOW())"
		database.insert([
			company_id,
			film
		], False, True)  # print("[{}@{}] Movie Company created: {} - {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, company_id, film))
	else:
		if database.results.get('updated_at').astimezone(time_zone) > datetime.now(time_zone) - timedelta(days=days):
			return
		database.last_query = "UPDATE companies_movies SET updated_at = NOW() WHERE company_id = %s AND movie_id = %s"
		database.update([
			company_id,
			film
		], False, True)  # print("[{}@{}] Movie Company updated: {} - {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, company_id, film))


def handle_countries(database, countries, film):
	for country in countries:
		country_info = get_country_info(country.get('iso_3166_1'))
		if country_info is None:
			database.last_query = "SELECT * FROM countries WHERE iso_code = %s"
			database.select([
				country.get('iso_3166_1')
			], False, False)
			if database.results is not None:
				handle_countries_movies(database, {
					'type': 'App\\Models\\Movie',
					'id': film
				}, country.get('iso_3166_1'))
			continue
		database.last_query = "SELECT * FROM countries WHERE iso_code = %s"
		database.select([
			country.get('iso_3166_1')
		], False, False)
		if database.results is None:
			database.last_query = "INSERT INTO countries (code, name, iso_code, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())"
			database.insert([
				country_info.get('cca3'),
				country.get('name'),
				country.get('iso_3166_1')
			], False, True)  # print("[{}@{}] Country created: {}\n{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, country.get('name'), country_info))
		else:
			if time_zone.localize(database.results.get('updated_at')) > datetime.now(time_zone) - timedelta(days=days):
				continue
			database.last_query = "UPDATE countries SET updated_at = NOW() WHERE iso_code = %s"
			database.update([
				country.get('iso_3166_1')
			], False, True)  # print("[{}@{}] Country updated: {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, country.get('name')))
		handle_countries_movies(database, {
			'type': 'App\\Models\\Movie',
			'id': film
		}, country.get('iso_3166_1'))


def handle_countries_movies(database, entry, country):
	database.last_query = "SELECT * FROM entries_countries WHERE entry_type = %s AND entry_id = %s AND country_code = %s"
	database.select([
		entry.get('type'),
		entry.get('id'),
		country
	], False, False)
	if database.results is None:
		database.last_query = "INSERT INTO entries_countries (entry_type, entry_id, country_code, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())"
		database.insert([
			entry.get('type'),
			entry.get('id'),
			country
		], False, True)
	else:
		if database.results.get('updated_at').astimezone(time_zone) > datetime.now(time_zone) - timedelta(days=days):
			return
		database.last_query = "UPDATE entries_countries SET updated_at = NOW() WHERE entry_type = %s AND entry_id = %s AND country_code = %s"
		database.update([
			entry.get('type'),
			entry.get('id'),
			country
		], False, True)


def get_country_info(code_3166_1):
	if code_3166_1 is None or not code_3166_1:
		return None
	country_url = "https://restcountries.com/v3.1/alpha/{}".format(code_3166_1)
	try:
		response = requests.get(country_url).json()
		# print("\nVisiting country code {}".format(code_3166_1))
		time.sleep(1)
		return response[0] if response else None
	except KeyError as e:
		print("\nRequested: {}\nError: {}".format(country_url, e))
		return None
	except requests.exceptions.JSONDecodeError as json_exception:
		print("\nRequested: {}\nError: {}".format(country_url, json_exception))
		return None


def fix_gender(gender):
	if not gender:
		return None
	return gender == 2


def create_time_object():
	"""
	Creates a datetime.time object from integer hour, minute, and second components.

	Returns:
		A datetime.time object representing the specified time.
	"""
	current_time = datetime.now(time_zone)
	current_year = current_time.year
	current_month = current_time.month
	current_day = current_time.day
	try:
		current_hour = current_time.hour if current_time.hour > 9 else np.random.randint(10, 24)
		current_minute = current_time.minute if current_time.minute > 9 else np.random.randint(10, 60)
		current_second = current_time.second if current_time.second > 9 else np.random.randint(10, 60)
		# The datetime.time constructor takes the components directly.
		return datetime(year=current_year, month=current_month, day=current_day, hour=current_hour, minute=current_minute, second=current_second)
	except ValueError as value_error:
		print(f"Error creating time object: {value_error}")
		print("Please ensure hour is between 0-23, and minute/second are between 0-59.")
		return None


def create_movie_time(film_info):
	movie_time = create_time_object()
	if film_info.get("release_date"):
		return datetime.strptime(film_info.get("release_date"), "%Y-%m-%d").replace(hour=movie_time.hour, minute=movie_time.minute, second=movie_time.second)
	return datetime.now(time_zone)


def fix_date(entry_date, entry_format="%Y-%m-%d"):
	date_object = None
	if not entry_date:
		return None
	try:
		if entry_date is not None and not isinstance(entry_date, datetime):
			date_object = datetime.strptime(entry_date, entry_format)
	except ValueError as value_error:
		print("[MiscFunctions:56]\tEntry Date: {}\n[Misc:36]\tEntry Type: {}\n[Misc:36]\tValueError: {}".format(entry_date, type(entry_date).__name__, value_error))
		if isinstance(entry_date, str):
			return fix_date(entry_date, "%d/%m/%Y")
		else:
			print("[MiscFunctions:60]".format(entry_date))
			raise ValueError("entry_date must be a string")
	return date_object.date() if date_object is not None else None


def when_frame_ends(time_one, size_one, frame_name):
	velocity = (size_one - frame_name.shape[0]) / (datetime.now(time_zone) - time_one).seconds if (datetime.now(time_zone) - time_one).seconds > 0 else 1
	time_end = frame_name.shape[0] / velocity if velocity > 0 else datetime.now(time_zone).second
	return (datetime.now(time_zone) + timedelta(seconds=time_end)).strftime(date_format)
