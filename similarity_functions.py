from movie_functions import *


def calculate_similarity(connection, item_one, info_one, item_two, info_two):
	keywords_one = info_one.get('keywords')
	keywords_two = info_two.get('keywords')
	vector_one, vector_two = normalise_vectors(info_one.get('genres'), [], info_two.get('genres'), [])
	vector_one, vector_two = normalise_vectors(keywords_one.get('keywords'), vector_one, keywords_two.get('keywords'), vector_two)

	credits_one = info_one.get('credits')
	credits_two = info_two.get('credits')

	vector_one, vector_two = normalise_vectors(credits_one.get('cast'), vector_one, credits_two.get('cast'), vector_two)

	people_one = [person for person in credits_one.get('crew') if person.get('department') in ('Writing', 'Directing')]
	people_two = [person for person in credits_two.get('crew') if person.get('department') in ('Writing', 'Directing')]
	vector_one, vector_two = normalise_vectors(people_one, vector_one, people_two, vector_two)

	vector_one, vector_two = normalise_vectors(info_one.get('production_companies'), vector_one, info_two.get('production_companies'), vector_two)
	normal_one = np.linalg.norm(vector_one)
	normal_two = np.linalg.norm(vector_two)
	try:
		similarity = np.dot(vector_one, vector_two) / (float(normal_one) * float(normal_two))
		if similarity is np.nan:
			return None
	except RuntimeWarning:
		print("Vector One: {}".format(vector_one))
		print("Normal One: {}".format(np.linalg.norm(vector_one)))
		print("Vector Two: {}".format(vector_two))
		print("Normal Two: {}".format(np.linalg.norm(vector_two)))
		return None
	handle_similarity(connection, item_one.get('film'), item_two.get('film'), similarity.item())
	return similarity.item()


def handle_similarity(connection, movie_one, movie_two, similarity):
	connection.last_query = "SELECT * FROM similarities WHERE entry_type = %s AND entry_one = %s AND entry_two = %s"
	connection.select([
		'App\\Models\\Movie',
		movie_one,
		movie_two
	], False, False)
	if connection.results is None:
		connection.last_query = "INSERT INTO similarities (entry_one, entry_two, entry_type, similarity_score, created_at, updated_at) VALUES (%s, %s, %s, %s, NOW(), NOW())"
		connection.insert([
			movie_one,
			movie_two,
			'App\\Models\\Movie',
			similarity
		], False, True)
	else:
		if connection.results.get('updated_at').astimezone(time_zone) < datetime.now(time_zone) - timedelta(days=days):
			connection.last_query = "UPDATE similarities SET similarity_score = %s, updated_at = NOW() WHERE entry_type = %s AND entry_one = %s AND entry_two = %s"
			connection.update([
				similarity,
				'App\\Models\\Movie',
				movie_one,
				movie_two
			], False, True)


def normalise_vectors(original_one, list_one, original_two, list_two):
	source_entries = [entry.get('id') for entry in original_one] if original_one else []
	target_entries = [entry.get('id') for entry in original_two] if original_two else []

	# Get the unique join of both arrays
	joined = list(set(source_entries) | set(target_entries))

	# Iterate through the joined array and populate list_one and list_two
	for value in sorted(joined):
		list_one.append(1 if value in source_entries else 0)
		list_two.append(1 if value in target_entries else 0)

	# Return the two lists
	return list_one, list_two


def get_similar(connection, source_info):
	movie_frame = pd.DataFrame(columns=[
		'film',
		'link',
		'page',
		'title',
		'score',
		'rating',
		'votes',
		'created_at',
		'updated_at'
	]).set_index('link')
	# print("\n[{}@{}] Movie Frame:\n{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, movie_frame))
	page = 1
	url = "https://api.themoviedb.org/3/movie/{}/similar?language=en-US&page={}".format(source_info.get('id'), page)
	print("[{}@{} - {}] Requesting {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, source_info.get('title'), url))
	response = requests.get(url, headers=http_headers).json()
	pages = min(6, response.get('total_pages'))

	while page <= pages:
		url = "https://api.themoviedb.org/3/movie/{}/similar?language=en-US&page={}".format(source_info.get('id'), page)
		response = requests.get(url, headers=http_headers).json()
		if response.get('total_results') == 0:
			return movie_frame
		for result in response.get('results'):
			frame_size = movie_frame.shape[0]
			target_info = get_info(result.get('id'))
			if target_info is None:
				continue
			target_item = dict({
				'film': process(connection, target_info),
				'page': target_info.get('imdb_id'),
				'link': target_info.get('id')
			})

			if target_info.get('id') not in movie_frame.index:
				current_time = datetime.now(time_zone).replace(hour=np.random.randint(0, 23), minute=np.random.randint(0, 59), second=np.random.randint(0, 59))
				created_time = pd.to_datetime(target_info.get('release_date')).replace(hour=current_time.hour, minute=current_time.minute, second=current_time.second, tzinfo=current_time.tzinfo) if target_info.get('release_date') else pd.NaT
				movie_frame.at[target_info.get('id'), 'film'] = target_item.get('film')
				movie_frame.at[target_info.get('id'), 'page'] = target_item.get('page')
				movie_frame.at[target_info.get('id'), 'title'] = target_info.get('title')
				movie_frame.at[target_info.get('id'), 'rating'] = target_info.get('vote_average')
				movie_frame.at[target_info.get('id'), 'votes'] = target_info.get('vote_count')
				movie_frame.at[target_info.get('id'), 'score'] = target_info.get('popularity')
				movie_frame.at[target_info.get('id'), 'created_at'] = created_time.strftime(date_format) if created_time is not pd.NaT else None  # Remove .date() from pd.to_datetime(target_info.get('released')) to keep the value in datetime64[ns] format
				movie_frame.at[target_info.get('id'), 'updated_at'] = datetime.now(time_zone).strftime(date_format)

			target_credits = target_info.get('credits')
			for person in target_credits.get('cast'):
				person_films = get_person_films(connection, person.get('id'))
				if not person_films.empty:
					movie_frame = pd.concat([movie_frame.copy(), person_films])
				print("[{}@{} - {}] Just added filmography of {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person.get('original_name'), person.get('name')), end='{}\r'.format('.' * 8))

			left_rounds = "{} left".format(modulus - movie_frame.shape[0] % modulus) if movie_frame.shape[0] % modulus > 0 else "{} rounds".format(int(movie_frame.shape[0] / modulus))
			print("[{}@{} - Page {} / {}] Similar Frame has {} movies ({}) after `{}`{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, str(page).rjust(2, " "), pages, str(movie_frame.shape[0]).zfill(3), left_rounds,
				result.get('title'), '.' * 8), end='\n' if frame_size == movie_frame.shape[0] else '...\n')

			connection.last_query = "SELECT * FROM links WHERE link IN (%s, %s) AND entry_type = %s"
			connection.select([
				source_info.get('id'),
				target_info.get('id'),
				'App\\Models\\Movie'
			], False, True)
			source_entry = next((item for item in connection.results if item.get('link') == source_info.get('id')), None)
			target_entry = next((item for item in connection.results if item.get('link') == target_info.get('id')), None)
			source_item = dict({
				'film': source_entry.get('entry_id'),
				'link': source_info.get('id'),
				'page': source_entry.get('page')
			})
			target_item = dict({
				'film': target_entry.get('entry_id'),
				'link': target_info.get('id'),
				'page': target_entry.get('page')
			})
			score = calculate_similarity(connection, item_one=source_item, item_two=target_item, info_one=source_info, info_two=target_info)
			connection.last_query = "INSERT INTO similarities (entry_one, entry_two, entry_type, similarity_score, created_at, updated_at) VALUES (%s, %s, 'App\\Models\\Movie', %s, now(), now()) ON CONFLICT ON CONSTRAINT movie_similarities_entries_unique DO UPDATE SET similarity_score = %s, updated_at = now()"
			connection.insert([
				source_item.get('film'),
				target_item.get('film'),
				score,
				score
			], False, True)

			if movie_frame.shape[0] % modulus == 0:
				movie_frame.sort_index(inplace=True)
				print("\n[{}@{}] These movies will be processed\r{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, movie_frame) if not movie_frame.empty else "\r")
			time.sleep(1)
		page += 1
	movie_frame.sort_index(inplace=True)
	return movie_frame
