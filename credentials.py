import json
from datetime import datetime, timedelta

import numpy as np
import pytz

connection_info = {
	'database': 'movies',
	'hostname': '',
	'port': 5432,
}

http_headers = {
	"Accept": "application/json",
	"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIxY2Q3ZDgzMjkzM2EzZjhjYjBjOTU2YWM3MDk2NGU1ZiIsIm5iZiI6MTQxNTI3Mjc0My42MTQsInN1YiI6IjU0NWI1OTI3YzNhMzY4NTM1MzAwMTE2NCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.ilgTG99gFhuHw_j2ukmiOqOCQbLRA4ZWNXClKu5PE2I",
	"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.0; Windows 98;)",
}

# Open the file, read the JSON content, and close it
try:
	with open("next_file.json", "r", encoding="utf-8") as next_file:
		next_item = json.load(next_file)
except FileNotFoundError:
	next_item = {}

# Get a random time zone
time_zone = pytz.timezone(np.random.choice(pytz.common_timezones))

# Set the date format
date_format = "%Y-%m-%d %H:%M:%S"

# Get the current day of the month
days = datetime.now(time_zone).day

# Get the current day of the year
# days = datetime.now(time_zone).timetuple().tm_yday

# When does a movie is considered recently updated?
recent_time = datetime.now(time_zone) - timedelta(days=days)

modulus = 48

long_links = {
	123566: 'https://vk.com/club159427771',
	247245: 'https://www.arte.tv/fr/videos/arte-concert/fr/video/Pixies_a_l_Olympia/',
	414180: None,
	429193: None,
	426994: None,
	754265: 'https://www.emmystork.com',
	802372: 'https://www.amazon.com/People-We-Hate-At-Wedding/dp/B0B8TSS15B',
	906255: None,
	330182: 'https://www.europeanfilmgateway.eu/de/search-efg/detail',
	1221035: None,
}