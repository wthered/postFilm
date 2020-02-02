import psycopg2 as psy
import pandas as pd
import time

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()

movie_data = pd.read_csv('/tmp/ml-20m/movies.csv', header=1)

output = open('/tmp/ml-latest/movies.sql', 'w+')

start = int(time.time())

for index, row in movie_data.iterrows():
	genres = row[2].split('|')
	movie = str(row[0]).zfill(7)
	for genre in genres:
		if genre == '(no genres listed)':
			genre = 'Unknown'
		genre_query = "INSERT INTO movies.titles_genres (select %s,genre_id from movies.genres where genre = %s) on conflict do nothing;"
		if genre == 'IMAX':
			cur.execute(genre_query, [movie, genre])
		else:
			cur.execute(genre_query, [movie, genre.capitalize()])
		print(cur.query)
		conn.commit()
		output.write(str(cur.query) + "\n")

	movie_query = "INSERT INTO movies.titles (title_id,title) values (%s,%s) on conflict do nothing;"
	cur.execute(movie_query, [movie, row[1]])
	# conn.commit()
	print(cur.query)
	output.write(str(cur.query) + "\n")

# finish = int(time.time())
print("Was inserting for {} seconds".format(int(time.time()) - start))
