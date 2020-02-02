# userId,movieId,rating,timestamp
import psycopg2 as psy
import pandas as pd
import random
import time

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()

movie_ratings = pd.read_csv('/tmp/ml-latest/ratings.csv', header=1)

start = int(time.time())

for index, row in movie_ratings.iterrows():
	ratings_query = "INSERT INTO movies.ratings VALUES (%s,%s,%s,%s) on conflict (user_id, title_id) do update set unixtime = %s;"
	unix_time = random.randint(1024, int(time.time()))
	cur.execute(ratings_query, [row[0], row[1], row[2], row[3], unix_time])
	conn.commit()
	print(cur.query)

print("That whole thing lasted {} seconds".format(int(time.time()) - start))
