import time
import random

import pandas as pd
import psycopg2 as psy

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()

movie_ratings = pd.read_csv('/tmp/users.csv', header=None)

start = int(time.time())

for index, row in movie_ratings.iterrows():

    credentials_q = "INSERT INTO movies.credentials VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING;"
    cur.execute(credentials_q, [row[1], row[2], row[3], row[4]])
    conn.commit()
    print(cur.query)

    users_q = "INSERT INTO movies.userinfo VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;"

    if row[10] < 0 or row[10] == 0:
        row[10] = random.randint(1, 31)
    else:
        row[10] = (row[10] % 31) + 1
    cur.execute(users_q, [row[0], row[1], row[5], row[6], row[7], row[8], row[9], row[10]])
    conn.commit()
    print(cur.query)

    userdata_q = "INSERT INTO movies.userdata VALUES (%s,%s,%s) ON CONFLICT DO NOTHING;"
    cur.execute(userdata_q, [row[1], row[11], row[12]])
    conn.commit()
    print(cur.query)

print("That whole thing lasted {} seconds".format(int(time.time()) - start))
