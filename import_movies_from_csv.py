# This file is in the todo list
# This file is left for later.

import pandas as pd
import requests
from bs4 import BeautifulSoup

from credentials import http_headers


# conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
# cur = conn.cursor()

movie_plan = 1
movies = pd.read_csv('/tmp/watchlist.csv', header=None, engine='python', delimiter=',')


def get_duration(duration):
	if ' ' in duration:
		a = duration.split()
		hours = int(a[0][:-1])
		minutes = int(a[1][:-3])
		return 60 * hours + minutes
	else:
		return int(duration[:-3])


def get_release_dates():
	date_link = "https://www.imdb.com/title/{}/releaseinfo".format(row[0])
	date_page = requests.get(date_link, headers={
		'User-Agent': http_headers['User-Agent']
	})
	date_soup = BeautifulSoup(date_page.text, 'html.parser')
	for date_mono in date_soup.find_all('tr', class_='odd'):
		if 'Greece' in date_mono.text.strip():
			# print(date_mono.find_all('tr', class_='release_date'))
			print(date_mono.find_all('tr', class_='release_date'))
	print('\n** End of Odd **\n')
	for date_even in date_soup.find_all('tr', class_='even'):
		if 'Greece' in date_even.text.strip():
			print(date_even.find_all('tr', class_='release_date'))
	print('\n** End of Even **\n')
	print('\n** End of World **\n')
	pass


for index, row in movies.iterrows():
	movie_page = "https://www.imdb.com/title/{}".format(row[0])
	request_headers = {
		'User-Agent': http_headers['User-Agent']
	}
	page = requests.get(movie_page, headers=request_headers)
	# message = "Title {movie} from year {movie_year}"
	# print(message.format(movie=row[0], movie_year=row[5]))
	soup = BeautifulSoup(page.text, 'html.parser')
	title_box = soup.find('div', attrs={
		"class": 'originalTitle'
	})
	year_box = soup.find('span', attrs={
		"id": 'titleYear'
	})
	genres_list = soup.find_all('span', class_="itemprop", itemprop="genre")
	movie_plot = soup.find('div', class_='summary_text')
	movie_rating = soup.find('span', class_='rating').text.strip()
	movie_duration = soup.find('time', itemprop='duration').text.strip()
	movie_critics = soup.find('span', class_="small", itemprop="ratingCount").text.split()
	print("**********************")
	try:
		original_title = title_box.text.strip()
		original_year = year_box.text.strip()
		print('Movie Title:', original_title[:-len('(original title)') - 1])
		print('Year:', original_year[-5:-1])
		for original_genre in genres_list:
			print("Genre is", original_genre.text.strip())
		print("Plot:", movie_plot.text.strip())
		print("Rating:", movie_rating[:-3])
		print("Duration:", get_duration(movie_duration))
		print("Movie Critics:", movie_critics[0].replace(",", ""))
		print("Release Dates:", get_release_dates())
	except AttributeError:
		movie_title = soup.title.text.strip()
		print('Greek Title:', movie_title)
	else:
		# print(soup.title.text.strip())
		pass
	print("**********************\n")
