# Reads From SQl which users have rating less than 20 movies and does update that "bug"

import psycopg2 as psy
import time
import random

aluminium_shared = "10.42.0.13"
aluminium_network = "192.168.1.13"
conn = psy.connect(host=aluminium_network, database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def get_users():
	query = "SELECT user_id,count(user_id) from movies.ratings GROUP BY user_id HAVING count(user_id) < 20 ORDER BY count(user_id),user_id LIMIT %s"
	cur.execute(query, [maximum])
	return cur.fetchall()


def get_movies():
	query = "SELECT title_id from movies.titles"
	cur.execute(query, [])
	return cur.fetchall()


maximum = 750
movies = get_movies()

for row in get_users():
	# movie = movies[random.randint(0, len(movies) - 1)][0]
	# print('{0} voted {1} movie(s)\t{2}\tNew Rating {3}'.format(row[0], row[1],movie, rating))
	for user in range(row[1], random.randint(25, 1024)):
		query = "INSERT INTO movies.ratings VALUES (%s,%s,%s,%s) ON CONFLICT (user_id,title_id) DO UPDATE SET unixtime = %s"
		try:
			values = [
				row[0],
				movies[random.randint(0, len(movies) - 1)][0],
				random.randint(0, 10) / 2,
				int(time.time()),
				int(time.time())
			]
			cur.execute(query, values)
			conn.commit()
			print(cur.query)
		except psy.Error as err:
			print(']Line 45]:\t{1} Error: {0}'.format(err.pgerror, err.pgcode))
			pass
		except KeyError:
			print('[Line 48]\tKeyError Exception')
			pass
		else:
			continue
