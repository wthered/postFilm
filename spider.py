from database import *
from similarity_functions import *
from credentials import *
import pandas as pd


def create_frame(database):
	"""
	Create a DataFrame to store movie information.
	"""
	# the_frame = pd.DataFrame(columns=['film', 'link', 'page', 'title', 'score', 'rating', 'votes', 'created_at', 'updated_at']).set_index('link')
	the_frame = pd.read_csv('movie_file.csv').set_index('link')
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
	the_frame = the_frame[['film', 'page', 'title', 'score', 'rating', 'votes', 'created_at', 'updated_at']]

	while not the_frame.empty:
		for _ in the_frame.index:
			frame_index = np.random.randint(the_frame.shape[0])
			frame_entry = the_frame.index[frame_index]
			source_info = get_info(frame_entry)
			if source_info is None:
				the_frame.drop(frame_entry, inplace=True)
				continue
			source_item = {
				'film': process(database, source_info),
				'page': source_info.get('imdb_id'),
				'link': source_info.get('id')
			}
			print("[{}@{} - {}/{}] Processed {} or `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, frame_index + 1, the_frame.shape[0], source_info.get('title'), source_item))

			movie_credits = source_info.get('credits')
			for person_item in movie_credits.get('cast'):
				person_info = get_person_info(person_item.get('id'))
				if person_info is None:
					continue
				the_frame = pd.concat([
					the_frame.copy(),
					get_person_films(db, person_info.get('id'))
				], axis=0).sort_index()
				print("[{}@{}] Movies Frame after joining filmography\n{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, the_frame))

			if source_info.get('belongs_to_collection'):
				the_collection = source_info.get('belongs_to_collection')
				collection_response = requests.get('https://api.themoviedb.org/3/collection/{}?language=el-GR'.format(the_collection.get('id')), headers=http_headers).json()
				for collection_part in collection_response.get('parts'):
					db.last_query = "SELECT * FROM movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.entry_id = %s"
					db.select([
						'App\\Models\\Movie',
						collection_part.get('id')
					], True, False)
					if db.results is None or collection_part.get('id') not in the_frame.index:
						part_info = get_info(collection_part.get('id'))
						if part_info is None:
							continue
						part_item = {
							'film': process(db, part_info),
							'page': part_info.get('imdb_id'),
							'link': part_info.get('id')
						}
						print("[{}@{}] Added {} or {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, part_info.get('title'), part_item))

			db.last_query = "UPDATE links SET visited = True, updated_at = NOW() WHERE entry_type = %s AND link = %s"
			db.update([
				'App\\Models\\Movie',
				source_info.get('id')
			], False, True)
			# Drop the source movie from DataFrame
			the_frame.drop(frame_entry, inplace=True)

			# Write the updated DataFrame into file
			the_frame.to_csv('movie_file.csv', index=True)
		db.last_query = "SELECT entry_id AS film, page, link, created_at, updated_at FROM links WHERE entry_type = %s AND visited = %s LIMIT %s"
		db.select([
			'App\\Models\\Movie',
			False,
			modulus * datetime.now(time_zone).day
		], True, True)
		the_frame = pd.DataFrame(db.results).set_index('link')
	return the_frame


def process_local():
	local_frame = pd.read_csv('movie_file.csv').set_index('link')
	start_time = datetime.now(time_zone)
	start_size = local_frame.shape[0]
	if local_frame.empty:
		db.last_query = "SELECT entry_id AS film, page, link, m.title, m.score, m.rating, NULL AS votes, m.release_date, m.created_at, l.updated_at FROM public.movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.visited = %s LIMIT %s"
		db.select([
			'App\\Models\\Movie',
			False,
			modulus
		], True, True)
		local_frame = pd.DataFrame(db.results).set_index('link').fillna(pd.NA)
	for _ in local_frame.index:
		local_index = np.random.randint(local_frame.shape[0])
		local_entry = local_frame.index[local_index]
		local_info = get_info(local_entry)
		print("[{}@{} - {} / {}] Start processing {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, local_index + 1, local_frame.shape[0], local_info.get('title') if local_info is not None else ''))
		if local_info is None:
			db.last_query = "SELECT entry_id AS film, page, link, m.title, m.score, m.rating, NULL AS votes, m.release_date, m.created_at, l.updated_at FROM public.movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.link = %s"
			db.select([
				'App\\Models\\Movie',
				local_entry
			], True, False)
			if db.results is None:
				db.last_query = "DELETE FROM movies WHERE id IN (SELECT entry_id FROM links WHERE entry_type = %s AND link = %s)"
				db.delete([
					'App\\Models\\Movie',
					local_entry.item()
				], True, True)
				db.last_query = "DELETE FROM links WHERE entry_type = %s AND link = %s"
				db.delete([
					'App\\Models\\Movie',
					local_entry.item()
				], True, True)
				local_frame.drop(local_entry, inplace=True)
				next_item.update({
					'film': modulus
				})
			continue
		local_item = dict({
			'film': process(db, local_info),
			'link': local_info.get('id'),
			'page': local_info.get('imdb_id')
		})

		local_credits = local_info.get('credits')
		for person in local_credits.get('cast'):
			person_filmography = get_person_films(db, person.get('id'))
			if not person_filmography.empty:
				local_frame = pd.concat([
					local_frame.copy(),
					person_filmography
				], axis=0).sort_index()
			print("[{}@{} - {}] Added filmography of `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, person.get('original_name'), person.get('name')), end="{}\r".format('.' * 8))

		local_frame = pd.concat([
			local_frame.copy(),
			get_similar(db, local_info)
		], axis=0).sort_index()
		print("[{}@{}] Similar movies of `{}`\n{}".format(when_frame_ends(start_time, start_size, local_frame), time_zone.zone, local_info.get('title'), local_frame) if not local_frame.empty else "\r")
		try:
			if local_entry in local_frame.index:
				local_frame.drop(local_entry, errors='ignore', inplace=True)
		except KeyError:
			pass
		local_frame.sort_index(inplace=True)
		local_frame.to_csv('movie_file.csv', index=True)
		print("[{}@{} - {} / {}] End of processing `{}` or {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, local_index + 1, local_frame.shape[0], local_info.get('title'), local_item))
	return local_frame


if __name__ == "__main__":
	db = Database()
	movie_frame = process_local()
	create_frame(db)
