from database import *
from movie_functions import *


# Ορίζουμε τα dtypes σε ένα dictionary για να το χρησιμοποιούμε παντού
CSV_DTYPES = {
	'link': int,
	'film': 'Int64',
	'page': str,
	'votes': 'Int64',
	'score': float,
	'rating': float,
	'release_date': str  # Ορίζουμε ως string για να μην χτυπάει στη στήλη 7
}


def create_frame(database):
	"""
	Create a DataFrame to store movie information.
	"""
	try:
		# Εδώ ήταν το πρόβλημα, προσθέσαμε το dtype=CSV_DTYPES
		the_frame = pd.read_csv('movie_file.csv', dtype=CSV_DTYPES).set_index('link')
	except FileNotFoundError:
		the_frame = pd.DataFrame(columns=[
			'film',
			'page',
			'title',
			'score',
			'rating',
			'votes',
			'created_at',
			'updated_at'
		]).set_index('link')

	current_time = datetime.now(time_zone)
	if the_frame.empty:
		db.last_query = "SELECT entry_id AS film, page, link, m.title, m.score, m.rating, m.votes, m.created_at, l.updated_at FROM public.movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.visited = %s OR m.updated_at < %s LIMIT %s"
		db.select([
			'App\\Models\\Movie',
			False,
			current_time.replace(month=1, day=1).astimezone(time_zone),
			current_time.day
		], True, True)
		the_frame = pd.DataFrame(db.results).set_index('link')
		the_frame.sort_index(inplace=True)

	# Φιλτράρουμε τις στήλες που θέλουμε
	available_cols = [c for c in [
		'film',
		'page',
		'title',
		'score',
		'rating',
		'votes',
		'created_at',
		'updated_at'
	] if c in the_frame.columns]
	the_frame = the_frame[available_cols]

	while not the_frame.empty:
		for _ in the_frame.index:
			if the_frame.empty: break
			frame_index = np.random.randint(the_frame.shape[0])
			frame_entry = the_frame.index[frame_index]
			source_info = get_film_info(frame_entry)
			if source_info is None:
				the_frame.drop(frame_entry, inplace=True)
				continue
			source_item = {
				'film': process(database, source_info),
				'page': source_info.get('imdb_id'),
				'link': source_info.get('id')
			}
			print("[{}@{} - {}/{}] Processed {} or `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, frame_index + 1, the_frame.shape[0], source_info.get('title'), source_item))

			movie_credits = source_info.get('credits', {})
			for person_item in movie_credits.get('cast', []):
				person_info = get_person_info(person_item.get('id'))
				if person_info is None:
					continue

				new_films = get_person_films(db, person_info.get('id'))
				if not new_films.empty:
					the_frame = pd.concat([
						the_frame,
						new_films
					], axis=0)
					the_frame = the_frame[~the_frame.index.duplicated(keep='first')]
					the_frame.sort_index(inplace=True)

				print("[{}@{}] Added filmography of {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person_info.get('name')), end="\r")

			if source_info.get('belongs_to_collection'):
				the_collection = source_info.get('belongs_to_collection')
				# Χρήση get_film_info για να έχουμε τα σωστά headers/rate limit
				collection_url = 'https://api.themoviedb.org/3/collection/{}?language=el-GR'.format(the_collection.get('id'))
				r = requests.get(collection_url, headers=http_headers)
				if r.status_code == 200:
					collection_response = r.json()
					for collection_part in collection_response.get('parts', []):
						part_id = collection_part.get('id')
						if part_id not in the_frame.index:
							part_info = get_film_info(part_id)
							if part_info:
								process(db, part_info)
								print("[{}@{}] Added Collection Part: {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, part_info.get('title')))

			db.last_query = "UPDATE links SET visited = True, updated_at = NOW() WHERE entry_type = %s AND link = %s"
			db.update([
				'App\\Models\\Movie',
				int(source_info.get('id'))
			], False, True)

			the_frame.drop(frame_entry, errors='ignore', inplace=True)
			the_frame.to_csv('movie_file.csv', index=True)

		db.last_query = "SELECT entry_id AS film, page, link, created_at, updated_at FROM links WHERE entry_type = %s AND visited = %s LIMIT %s"
		db.select([
			'App\\Models\\Movie',
			False,
			modulus * datetime.now(time_zone).day
		], True, True)
		if db.results:
			the_frame = pd.DataFrame(db.results).set_index('link')
		else:
			break
	return the_frame


def process_local():
	try:
		local_frame = pd.read_csv('movie_file.csv', dtype=CSV_DTYPES).set_index('link')
	except FileNotFoundError:
		local_frame = pd.DataFrame(columns=[
			'film',
			'page',
			'title',
			'score',
			'rating',
			'votes',
			'release_date',
			'created_at',
			'updated_at'
		]).set_index('link')

	start_time = datetime.now(time_zone)
	if local_frame.empty:
		db.last_query = "SELECT entry_id AS film, page, link, m.title, m.score, m.rating, NULL AS votes, m.release_date, m.created_at, l.updated_at FROM public.movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.visited = %s LIMIT %s"
		db.select([
			'App\\Models\\Movie',
			False,
			modulus
		], True, True)
		if db.results:
			local_frame = pd.DataFrame(db.results).set_index('link')
			for col in [
				'film',
				'page',
				'title',
				'score',
				'rating',
				'votes',
				'release_date',
				'created_at',
				'updated_at'
			]:
				if col not in local_frame.columns and col != 'link':
					local_frame[col] = pd.NA
		else:
			print("[{}@{} No more movies to process.]".format(datetime.now(time_zone).strftime(date_format), time_zone.zone))
			return local_frame

	# processing_queue = list(local_frame.index)
	for local_entry in list(local_frame.index):
		# local_entry = local_frame.at[local_frame.index[frame_index]]
		# print("Local Entry is {}".format(local_entry))
		if local_entry not in local_frame.index: continue

		local_info = get_film_info(local_entry)
		if local_info is None:
			# Καθαρισμός αν η ταινία δεν υπάρχει πια στο API
			db.last_query = "DELETE FROM movies WHERE id IN (SELECT entry_id FROM links WHERE entry_type = %s AND link = %s)"
			db.delete([
				'App\\Models\\Movie',
				int(local_entry)
			], True, True)
			db.last_query = "DELETE FROM links WHERE entry_type = %s AND link = %s"
			db.delete([
				'App\\Models\\Movie',
				int(local_entry)
			], True, True)
			local_frame.drop(local_entry, errors='ignore', inplace=True)
			continue

		film_item = dict({
			'film': process(db, local_info),
			'link': local_entry,
			'page': fix_page(local_info.get('imdb_id')),
		})
		print("[{}@{} - {} / {}] Processing: {} {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, local_entry, local_frame.shape[0], local_info.get('title'), film_item))

		new_data_list = []
		local_credits = local_info.get('credits', {})
		for person in local_credits.get('cast', []):
			person_filmography = get_person_films(db, person.get('id'))
			if not person_filmography.empty:
				new_data_list.append(person_filmography)
			print("[{}@{} Adding filmography: {}]{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person.get('name'), '.' * 4), end="\r")

		similar_movies = get_similar(db, local_info)
		if not similar_movies.empty:
			new_data_list.append(similar_movies)

		if new_data_list:
			all_new = pd.concat(new_data_list, axis=0)
			local_frame = pd.concat([
				local_frame,
				all_new
			], axis=0)
			local_frame = local_frame[~local_frame.index.duplicated(keep='first')]

		local_frame.drop(local_entry, errors='ignore', inplace=True)
		local_frame.sort_index(inplace=True)
		local_frame.to_csv('movie_file.csv', index=True)

		print("[{}@{} Success: `{}` processed. Size: {}]".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, local_info.get('title'), local_frame.shape[0]))

	return local_frame


if __name__ == "__main__":
	db = Database()
	movie_frame = process_local()
	create_frame(db)
