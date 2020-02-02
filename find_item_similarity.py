import numpy as np
import psycopg2 as psy

aluminium_shared = "10.42.0.13"
aluminium_hosted = "192.168.1.13"

database_host = aluminium_hosted
database_name = 'aluminium'
database_user = "ntina23gr"
database_pass = "!p9lastiras"


def connect():
	database_connection = psy.connect(host=aluminium_hosted, database=database_name, user=database_user, password=database_pass)
	return database_connection


# Query 1
def find_all_users(genre=1):
	query = "SELECT * FROM movies.userinfo ORDER BY user_id LIMIT 25"
	try:
		cursor.execute(query, [genre])
		return np.array(cursor.fetchall())
	# return cursor.fetchall()
	except psy.Error as error:
		print("[Line 50]\t{}".format(error))


def find_all_items(genre=1):
	query = "SELECT * FROM movies.ratings WHERE title_id IN (SELECT title_id FROM movies.titles_genres WHERE genre_id = %s) LIMIT 5"
	try:
		cursor.execute(query, [genre])
		return np.array(cursor.fetchall())
	except (psy.Error, psy.DatabaseError) as database_error:
		print("[Line 39]\t{}".format(database_error))
	pass


def films_rated_by(user):
	query = "SELECT * FROM movies.ratings WHERE user_id = %s ORDER BY title_id LIMIT 5"
	try:
		cursor.execute(query, [user])
		return np.array(cursor.fetchall())
	except (psy.Error, psy.DatabaseError) as database_error:
		print("[Line 48]\t{}".format(database_error))
		pass


def main():
	users = find_all_users(1)
	for user_index in range(len(users)):
		user_info = np.array(users[user_index])
		user_id = int(user_info[0])
		# print(user_index, user_id, user_info[1])
		for film in films_rated_by(user_id):
			print(user_id, int(film[1]), float(film[2]))


if __name__ == "__main__":
	connection = connect()
	print("Using Client Encoding: {}".format(connection.encoding))
	connection.set_client_encoding('UNICODE')
	print("Setting Client Encoding as {}".format(connection.encoding))
	cursor = connection.cursor()
	main()
