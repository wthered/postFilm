from database import Database
from tv_functions import *
import time


if __name__ == "__main__":
	db = Database()
	page = 1
	pages = 4
	while page <= pages:
		daily_series = get_daily_series(page)
		pages = min(8, daily_series.get('total_pages'))
		for result in daily_series.get('results'):
			print("[Page {} / {}] Seen TV Series #{}".format(page, pages, result.get('id')))
			series_info = get_series_info(result.get('id'))
			if series_info is None:
				continue
			process_tv(db, series_info)
			time.sleep(1)
		page += 1
		time.sleep(1)
