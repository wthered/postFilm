from database import Database
from misc_functions import *
from movie_functions import *


def process_local():
	local_frame = pd.read_csv('movie_file.csv').set_index('link')
	start_time = datetime.now(time_zone)
	start_size = local_frame.shape[0]
	if local_frame.empty:
		return None
	while not local_frame.empty:
		local_index = np.random.randint(local_frame.shape[0])
		local_entry = local_frame.index[local_index]
		local_info = get_film_info(local_entry)
		if local_info is None:
			db.last_query = "SELECT entry_id AS film, page, link, m.title, m.score, m.rating, NULL AS votes, m.release_date, m.created_at, l.updated_at FROM public.movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.link = %s"
			db.select([
				'App\\Models\\Movie',
				local_entry.item()
			], True, False)
			if db.results is None:
				db.last_query = "DELETE FROM movies WHERE id IN (SELECT entry_id FROM links WHERE entry_type = %s AND link = %s)"
				db.delete([
					'App\\Models\\Movie',
					local_entry.item()
				], False, True)
				db.last_query = "DELETE FROM links WHERE entry_type = %s AND link = %s"
				db.delete([
					'App\\Models\\Movie',
					local_entry.item()
				], False, True)
				local_frame.drop(local_entry, inplace=True)
			continue
		local_item = dict({
			'film': process(db, local_info),
			'link': local_info.get('id'),
			'page': local_info.get('imdb_id')
		})
		print("[{}@{} - {} / {}] Processed {} or `{}`".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, str(local_index + 1).zfill(len(str(local_frame.shape[0]))), local_frame.shape[0], local_item, local_info.get('title')))
		local_frame.drop(local_entry, inplace=True)
		if local_frame.shape[0] % modulus == 0:
			local_frame.to_csv('movie_file.csv', index=True)
			print("[{}@{}] These movies will be processed\n{}".format(when_frame_ends(start_time, start_size, local_frame), time_zone.zone, local_frame))
		time.sleep(1)
	local_frame.to_csv('movie_file.csv', index=True)
	return None


if __name__ == "__main__":
	db = Database()
	process_local()
	page = 1
	max_pages = 496
	pages = max_pages
	dots = 0
	while page <= pages:
		available = [
			"https://api.themoviedb.org/3/movie/now_playing?language=el-GR&page={}",
			"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=el-GR&page={}",
			"https://api.themoviedb.org/3/movie/popular?language=el-GR&page={}",
			"https://api.themoviedb.org/3/movie/top_rated?language=el-GR&page={}",
			"https://api.themoviedb.org/3/movie/upcoming?language=el-GR&page={}",
			"https://api.themoviedb.org/3/trending/movie/day?language=el-GR&page={}",
			"https://api.themoviedb.org/3/trending/movie/week?language=el-GR&page={}",
		]
		url = available[datetime.now(time_zone).day % len(available)].format(page)
		print("[{}@{} - Page {} / {}] Requesting {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, str(page).rjust(len(str(pages)), '_'), pages, url))
		response = requests.get(url, headers=http_headers).json()
		pages = min(max_pages, response.get('total_pages'))
		for result in response.get('results'):
			db.last_query = "SELECT * FROM links WHERE link = %s"
			db.select([
				result.get('id')
			], False, False)
			if db.results is not None and time_zone.localize(db.results.get('updated_at')) > datetime.now(time_zone) - timedelta(days=days):
				film_item = {
					'film': db.results.get('film'),
					'link': result.get('id'),
					'page': db.results.get('imdb_id')
				}
				print("[{}@{} - Page {} / {}] Skipping {}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, str(page).rjust(len(str(pages))), pages, film_item, end='\r'))
				continue
			film_info = get_info(result.get('id'))
			if film_info.get('belongs_to_collection'):
				the_collection = film_info.get('belongs_to_collection')
				collection_response = requests.get('https://api.themoviedb.org/3/collection/{}?language=el-GR'.format(the_collection.get('id')), headers=http_headers).json()
				for collection_part in collection_response.get('parts'):
					db.last_query = "SELECT * FROM movies m INNER JOIN links l ON m.id = l.entry_id WHERE l.entry_type = %s AND l.entry_id = %s"
					db.select([
						'App\\Models\\Movie',
						collection_part.get('id')
					], False, False)
					if db.results is None:
						part_info = get_info(collection_part.get('id'))
						if part_info is None:
							print("[main.py:96] There is no link to {} as part of a collection".format(collection_part.get('id')))
							continue
						part_item = dict({
							'film': process(db, part_info),
							'page': part_info.get('imdb_id'),
							'link': part_info.get('id')
						})
			try:
				dots = max(dots, len(film_info.get('title')) % datetime.now(time_zone).second) if datetime.now(time_zone).second > 0 else 4
			except TypeError:
				print("Type Error occurred for {}".format(result.get('id')))
				break
			film_item = {
				'film': process(db, film_info),
				'link': result.get('id'),
				'page': film_info.get('imdb_id')
			}
			print("[{}@{} - Page {} / {}] Processed {} or `{}`{}".format(datetime.now(time_zone).strftime(date_format), time_zone.zone, str(page).rjust(len(str(pages))), pages, film_item, film_info.get('title'), '.' * dots))
			time.sleep(max(1, page % 4))
		page += 1
		time.sleep(max(1, page % 4))
