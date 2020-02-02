import math
import time

import numpy as np
import psycopg2 as psy

aluminium_shared = "10.42.0.13"
aluminium_hosted = "192.168.1.13"

database_host = aluminium_hosted
database_name = 'aluminium'
database_user = "ntina23gr"
database_pass = "!p9lastiras"

connection = psy.connect(host=aluminium_hosted, database=database_name, user=database_user, password=database_pass)
print("Using Client Encoding: {}".format(connection.encoding))
connection.set_client_encoding('UNICODE')
print("Setting Client Encoding as {}".format(connection.encoding))
cursor = connection.cursor()


# Query 1
def find_all_users(genre=1):
	query = "SELECT * FROM movies.userinfo WHERE fav_genre = %s ORDER BY user_id"
	try:
		cursor.execute(query, [genre])
		# return np.array(cursor.fetchall())
		return cursor.fetchall()
	except psy.Error as error:
		print("[Line 50]\t{}".format(error))


def find_user_average(user_identification):
	query = "SELECT AVG(rating) FROM movies.ratings WHERE user_id = %s"
	try:
		cursor.execute(query, [user_identification])
		user_average = cursor.fetchall()
		return user_average[0][0]
	except psy.DatabaseError as database_error:
		print("[Line 34]{}".format(database_error.pgerror))
		pass


def find_user_ratings(user_id, movie_id):
	query = "SELECT * FROM movies.ratings WHERE user_id = %s AND title_id = %s"
	try:
		cursor.execute(query, [user_id, movie_id])
		return cursor.fetchall()
	except psy.DatabaseError as database_error:
		print("[Line 34]{}".format(database_error.pgerror))
		pass


def find_magnitude(user_id, user_average):
	query = "SELECT * FROM movies.ratings WHERE user_id = %s"
	summation = 0
	try:
		cursor.execute(query, [user_id])
		# results = cursor.fetchall()
		for result in cursor.fetchall():
			summation += math.pow(result[2] - user_average, 2)
		# print("{}\t{}\t{}".format(result[2], user_average, summation))
		return math.sqrt(summation)
	except psy.DatabaseError as database_error:
		print(database_error)
	pass


def similarity(source_user_id, target_user_id):
	query = "INSERT INTO movies.similarity_users (user_source, user_target, similarity, unixtime) VALUES (%s,%s,%s,%s) ON CONFLICT (user_source, user_target) DO UPDATE SET similarity = %s, unixtime = %s"
	try:
		parameters = [source_user_id, target_user_id, pearson_similarity, int(time.time()), similarity, int(time.time())]
		cursor.execute(query, parameters)
		if pearson_similarity is not 0:
			connection.commit()
		# print(cursor.mogrify(query, parameters))
		# print(cursor.query)
	except psy.DatabaseError as db_error:
		print(db_error)
	pass


users = find_all_users(1)
# for user in users:
# 	print(user)

source_rating = np.array(np.array([]))
target_rating = np.array(np.array([]))
pearson_similarity = 0


def find_common_movies():
	last_query = "SELECT * FROM movies.ratings WHERE user_id = %s AND title_id IN (SELECT title_id FROM movies.ratings WHERE user_id = %s)"
	try:
		cursor.execute(last_query, [user_source, user_target])
		return cursor.fetchall()
	except (psy.DatabaseError, psy.Error) as database_error:
		print(database_error)
		pass


for user_source_index in range(len(users)):
	user_source = int(users[user_source_index][0])
	user_source_average = find_user_average(user_source)
	source_vector_magnitude = find_magnitude(user_source, user_source_average)
	for user_target_index in range(user_source_index + 1, len(users)):
		nominator = 0
		pearson_similarity = 0
		temp_similarity = 0
		user_target = int(users[user_target_index][0])
		results = find_common_movies()
		user_target_average = find_user_average(user_target)
		# print("User #{} Source Average: {}".format(user_source, user_source_average))
		# print("User #{} Target Average: {}".format(user_target, user_target_average))
		for result_row in results:
			target_vector_magnitude = find_magnitude(user_target, user_target_average)
			try:
				source_rating = find_user_ratings(user_source, result_row[1])
				target_rating = find_user_ratings(user_target, result_row[1])
				nominator += (source_rating[0][2] - user_source_average) * (target_rating[0][2] - user_target_average)
				denominator = source_vector_magnitude * target_vector_magnitude
				pearson_similarity += nominator / denominator
				# pearson_similarity += temp_similarity
			except IndexError:
				print("User {} did not vote {}\t{}".format(user_source, result_row[1], source_rating))
				print("User {} did not vote {}\t{}".format(user_target, result_row[1], target_rating))
				continue
			except psy.InternalError as internal:
				print("[Line 120]\t{}".format(internal.pgerror))

		similarity(user_source, user_target)
		print("The Similarity of {} and {} is {}".format(user_source, user_target, pearson_similarity))
# print("*******************************************")

# all_users = find_all_users(25, 25)
#
# for user_source in range(len(all_users)):
# 	for user_target in range(1, len(all_users)):
# 		# Query 2
# 		last_query = "SELECT * FROM movies.ratings WHERE user_id = 1 AND title_id IN (SELECT title_id FROM movies.ratings WHERE user_id = %s)"
# 		parameters = [user_source, user_target]
# 		pass
