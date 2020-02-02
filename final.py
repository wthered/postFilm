# Calculates similarity of 2 movies for each pair of movies in database
# Using the Pearson Correlation Coefficient
import time

import psycopg2 as psy
from math import sqrt, pow

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def find_similarity():
	product = 0
	den = metro(ratings_source) * metro(ratings_target)
	if den == 0:
		return float(0)
	for source in range(0, len(users_source)):
		for target in range(0, len(users_target)):
			if users_source[source] == users_target[target]:
				a = ratings_source[source] - mean_source
				b = ratings_target[target] - mean_target
				product += a * b
	return product / den


def metro(vector):
	product = 0
	mean_vector = sum(vector) / len(vector)
	for element in range(0, len(vector)):
		product += pow(vector[element] - mean_vector, 2)
	return sqrt(product)


def get_ratings():
	users = []
	ratings = []
	query = "SELECT * FROM movies.ratings WHERE title_id = %s ORDER BY user_id"
	cur.execute(query, [movie])
	rows = cur.fetchall()
	for line in range(0, len(rows)):
		users.append(rows[line][0])
		ratings.append(rows[line][2])
	return users, ratings


def get_same_genre_movies(param):
	movies = []
	sql_query = "SELECT DISTINCT title_id FROM movies.titles_genres WHERE genre_id IN (SELECT genre_id FROM movies.titles_genres WHERE title_id = %s) AND title_id != %s ORDER BY title_id LIMIT %s"
	cur.execute(sql_query, [param, param, maximum])
	for movie in cur.fetchall():
		movies.append(movie[0])
	return movies


def get_movies(page, lim):
	query = "select * from movies.titles order by title_id offset %s limit %s"
	cur.execute(query, [page * lim, lim])
	movies = []
	for row in cur.fetchall():
		movies.append(row[0])
	return movies


start = int(time.time())
page = 2
maximum = 500
# movie_list = get_movies(page, maximum)
movie_list = get_same_genre_movies(160)

for movie in range(0, len(movie_list)):
	users_source, ratings_source = get_ratings()
	mean_source = sum(ratings_source) / len(ratings_source)
	for item in range(movie + 1, len(movie_list)):
		now = int(time.time())
		users_target, ratings_target = get_ratings()
		mean_target = sum(ratings_target) / len(ratings_target)
		similarity = find_similarity()

		distance_a_b = "INSERT INTO movies.distances values (%s,%s,%s,%s) on conflict (title_source,title_target) do update set unixtime = %s"
		parameters = [movie_list[movie], movie_list[item], similarity, now, now]
		cur.execute(distance_a_b, parameters)
		# print(type(cur.query))
		# print(cur.query)
		conn.commit()

		distance_b_a = "INSERT INTO movies.distances values (%s,%s,%s,%s) on conflict (title_source,title_target) do update set unixtime = %s"
		parameters = [movie_list[item], movie_list[movie], similarity, now, now]
		cur.execute(distance_b_a, parameters)
		conn.commit()

		print("Similarity of movie", movie_list[movie], "and", movie_list[item], "is", similarity)

finish = int(time.time())
print("Lasted", (finish - start), "seconds")
