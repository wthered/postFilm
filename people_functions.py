import pandas as pd

from misc_functions import *
from misc_functions import get_info


def find_next_person(database):
	database.last_query = "SELECT nextval('people_id_seq')"
	database.select([], False, False)
	# print("\nNext Person results: {}".format(database.results))
	next_person = database.results.get('nextval') if database.results else 1
	# print("\nNext Person from {} till {}".format(next_item.get('person', 4) - 3, next_person))
	for person in range(next_item.get('person', 4) - 3 if next_item.get('person', 5) > 3 else 1, next_person + 3):
		database.last_query = "SELECT * FROM people WHERE id = %s"
		database.select([
			person
		], False, False)
		# print("Find next Person results: {}".format(database.results))
		if database.results is None:
			next_item.update({
				'person': person
			})
			with open("next_file.json", "w") as next_person_file:
				json.dump(next_item, next_person_file, indent=4)
			return person
	return None


def get_person_info(person_link):
	visit_person = 'https://api.themoviedb.org/3/person/{}?language=el-GR'.format(person_link)
	response = {
		'page': None,
		'gender': None
	}
	try:
		response = requests.get(visit_person, headers=http_headers).json()
	except requests.exceptions.JSONDecodeError:
		print("Requested {}".format(visit_person))
	response['page'] = fix_page(person_link)
	response['gender'] = response.get('gender') == 2 if response.get('gender') is not None else None
	if response.get('success', True) is False:
		return None
	return response


def get_person_films(database, person_link):
	person_frame = pd.DataFrame(columns=[
		'film',
		'page',
		'link',
		'title',
		'score',
		'rating',
		'votes',
		'created_at',
		'updated_at'
	]).set_index('link')
	filmography_url = "https://api.themoviedb.org/3/person/{}/movie_credits?language=el-GR&include_adult=false".format(person_link)
	filmography = requests.get(filmography_url, headers=http_headers).json()
	for movie in filmography.get('cast'):
		movie_info = get_info(movie.get('id'))
		if movie_info is None:
			continue
		database.last_query = "SELECT EXISTS (SELECT 1 FROM links WHERE link = %s) AS existence"
		database.select([
			movie_info.get('id')
		], False, False)

		if database.results.get("existence") is False:
			release_date = datetime.strptime(movie_info.get('release_date', '').strip(), "%Y-%m-%d") if movie_info.get('release_date') else pd.NaT
			current_time = datetime.now(time_zone).replace(hour=np.random.randint(1, 23), minute=np.random.randint(1, 59), second=np.random.randint(1, 59))
			person_frame.loc[movie_info.get('id')] = {
				'film': None,
				'page': movie_info.get('imdb_id'),
				'title': movie_info.get('title'),
				'score': movie_info.get('popularity'),
				'rating': movie_info.get('vote_average'),
				'votes': movie_info.get('vote_count'),
				'created_at': release_date.replace(hour=current_time.hour, minute=current_time.minute, second=current_time.second).strftime(date_format) if release_date is not pd.NaT else pd.NaT,
				'updated_at': current_time.strftime(date_format),
			}
	return person_frame


def handle_people(database, movie_credits, movie):
	handle_cast(database, movie_credits.get('cast'), movie)
	handle_crew(database, movie_credits.get('crew'), movie)


def handle_cast(database, cast_people, movie):
	dots = 0
	name_lengths = [len(person.get('name')) % 10 for person in cast_people]
	last_person = next_item.get('person')
	for i, person in enumerate(cast_people):
		dots = max(dots, name_lengths[i])
		person_info = get_person_info(person.get('id'))
		if person_info is None:
			print("Personal Information (Personal Link: {}) not found".format(person.get('id')))
			database.last = "SELECT * FROM people WHERE link = %s"
			database.select([
				person.get('id')
			], True, False)
			if database.results is None:
				# So that we can update the next_item entry
				database.last_query = "DELETE FROM people WHERE link = %s"
				database.delete([
					person.get('id')
				], True, True)
				next_item.update({
					'person': max(last_person - 5, 5)
				})
				with open("next_file.json", "w") as next_cast_file:
					json.dump(next_item, next_cast_file, indent=4)
			continue
		if person_info.get('page') is None:
			database.last_query = "SELECT * FROM people WHERE link = %s AND page IS NULL"
			database.select([
				person.get('id')
			], False, False)
		else:
			database.last_query = "SELECT * FROM people WHERE link = %s OR page = %s"
			database.select([
				person.get('id'),
				person_info.get('page')
			], False, False)
		if database.results is None:
			person_id = find_next_person(database)
			person_data = [
				person_id,
				person.get('name'),
				person_info.get('place_of_birth'),
				person_info.get('birthday'),
				person_info.get('deathday'),
				person_info.get('profile_path'),
				person_info.get('biography'),
				person_info.get('gender'),
				person.get('popularity'),
				person.get('known_for_department'),
				person.get('id'),
				person_info.get('page'),
				person_info.get('homepage')
			]
			# print("[{}@{}] Create Cast Person {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person_id, person.get('name')), end='\r')
			database.last_query = "INSERT INTO people (id, name, place_of_birth, date_of_birth, date_of_death, photo, biography, gender, score, known_for, link, page, homepage, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
			database.insert(person_data, False, True)
		else:
			person_id = database.results.get('id')
			movie_time = pytz.UTC.localize(database.results.get('updated_at')).astimezone(time_zone)
			if movie_time > datetime.now(time_zone) - timedelta(days=days):
				continue
			person_data = [
				person.get('name'),
				person_info.get('place_of_birth'),
				person_info.get('birthday'),
				person_info.get('deathday'),
				person_info.get('profile_path'),
				person_info.get('biography'),
				person_info.get('gender'),
				person.get('popularity'),
				person.get('known_for_department'),
				person.get('id'),
				person_info.get('page'),
				person_info.get('homepage'),
				person_id
			]
			# print("[{}@{}] Update Cast Person `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person.get('name')), end='\r')
			database.last_query = "UPDATE people SET name = %s, place_of_birth = %s, date_of_birth = %s, date_of_death = %s, photo = %s, biography = %s, gender = %s, score = %s, known_for = %s, link = %s, page = %s, homepage = %s, updated_at = NOW() WHERE id = %s"
			database.update(person_data, False, True)

		person['character'] = process_character(person.get('character')) if len(person.get('character')) > 235 else person.get('character')
		handle_people_roles(database, {
			'entry_type': 'App\\Models\\Movie',
			'entry_id': movie
		}, {
			'person_type': 'App\\Models\\Actor',
			'person_id': person_id
		}, person)


def process_character(character, max_length=233):
	# Split the string using '/' as the separator
	parts = character.split('/')

	# Join the parts using ',' as the separator
	joined_string = ','.join(parts)

	# Check if the length exceeds the maximum allowed
	if len(joined_string) > max_length:
		# Truncate the string to fit within the limit
		joined_string = joined_string[:max_length]

	return "{} and many more&hellip;".format(joined_string)


def handle_people_roles(database, entry, role, person):
	# print("\n\n\n{}".format({"entry": entry, "role": role, "person": person}))
	database.last_query = "SELECT * FROM people_roles WHERE entry_type = %s AND entry_id = %s AND role = %s AND person_id = %s"
	database.select([
		entry.get('entry_type'),
		entry.get('entry_id'),
		role.get('person_type'),
		role.get('person_id')
	], False, False)
	if database.results is None:
		person_data = [
			entry.get('entry_type'),
			entry.get('entry_id'),
			role.get('person_type'),
			role.get('person_id'),
			person.get('character'),
			person.get('order'),
			person.get('known_for_department'),
			person.get('cast_id'),
			person.get('credit_id')
		]
		database.last_query = "INSERT INTO people_roles (entry_type, entry_id, role, person_id, character, \"order\", known_for, cast_id, credit_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
		database.insert(person_data, False, True)
	else:
		if pytz.UTC.localize(database.results.get('updated_at')).astimezone(time_zone) > datetime.now(time_zone) - timedelta(days=days):
			return
		person_data = [
			person.get('character'),
			person.get('order'),
			person.get('known_for_department'),
			person.get('cast_id'),
			person.get('credit_id'),
			entry.get('entry_type'),
			entry.get('entry_id'),
			role.get('person_type'),
			role.get('person_id')
		]
		print("[{}@{}] Update Person Role: `{}` as `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person.get('name'),
			person.get('character') if person.get('job') is None else "{} - {}".format(person.get('job'), person.get('department'))), end='\r')
		database.last_query = "UPDATE people_roles SET character = %s, \"order\" = %s, known_for = %s, cast_id = %s, credit_id = %s, updated_at = NOW() WHERE entry_type = %s AND entry_id = %s AND role = %s AND person_id = %s"
		database.update(person_data, False, True)


def handle_crew(database, crew_people, movie):
	crew_person = None
	for person in [person for person in crew_people if person.get('department') in [
		'Directing',
		'Writing'
	]]:
		person_info = get_person_info(person.get('id'))
		if person_info is None:
			print("Person link #{} seems not existant. Deleting".format(person.get('id')))
			database.last_query = "DELETE FROM people WHERE link = %s"
			database.delete([
				person.get('id')
			], True, True)
			continue
		database.last_query = "SELECT * FROM people WHERE link = %s"
		database.select([
			person.get('id')
		], False, False)
		if database.results is None:
			crew_person = find_next_person(database)
			person_data = [
				crew_person,
				person.get('name'),
				person_info.get('birthday'),
				person_info.get('deathday'),
				person_info.get('date_of_death'),
				person_info.get('biography'),
				person_info.get('gender'),
				person.get('popularity'),
				person.get('known_for_department'),
				person.get('id'),
				person_info.get('page'),
				person_info.get('homepage')
			]
			# print("[{}@{}] Create Crew Person: {} named {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, crew_person, person.get('name'), person_data), end='\r')
			database.last_query = "INSERT INTO people (id, name, place_of_birth, date_of_birth, date_of_death, biography, gender, score, known_for, link, page, homepage, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
			database.insert(person_data, False, True)
		else:
			crew_person = database.results.get('id')
			person_data = [
				person.get('name'),
				person_info.get('place_of_birth'),
				person_info.get('birthday'),
				person_info.get('deathday'),
				person_info.get('biography'),
				person_info.get('gender'),
				person.get('popularity'),
				person.get('known_for_department'),
				person.get('id'),
				person_info.get('page'),
				person_info.get('homepage'),
				crew_person
			]
			if time_zone.localize(database.results.get('updated_at')) < datetime.now(time_zone) - timedelta(days=days):
				# print("[{}@{}] Update Crew Person: {} named {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, crew_person, person.get('name'), person_data), end='\r')
				# print("[{}@{} - `{}` in {}] Crew updated: {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person.get('job'), person.get('department'), person.get('name')), end='\r')
				database.last_query = "UPDATE people SET name = %s, place_of_birth = %s, date_of_birth = %s, date_of_death = %s, biography = %s, gender = %s, score = %s, known_for = %s, link = %s, page = %s, homepage = %s, updated_at = NOW() WHERE id = %s"
				database.update(person_data, False, True)
		# print("[{}@{}] Crew processed {}{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, {'crew': crew_person, 'link': person_info.get('id'), 'page': person_info.get('page')}, ' ' * 8), end='\r')
		handle_people_roles(database, {
			'entry_type': 'App\\Models\\Movie',
			'entry_id': movie
		}, {
			'person_type': 'App\\Models\\{}'.format('Director' if person.get('department') == 'Directing' else 'Writer'),
			'person_id': crew_person
		}, person)  # time.sleep(1)
