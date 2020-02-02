# Reads Titles from database and updates titles

import psycopg2 as psy
import urllib.error
import urllib.request
import json
import time

conn = psy.connect(host="10.42.0.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def read_links(limit, movie_page):
    last_query = "SELECT * FROM movies.links ORDER BY title_id OFFSET %s LIMIT %s"
    cur.execute(last_query, [movie_page * limit, limit])
    return cur.fetchall()


def fetch(url):
    try:
        req = urllib.request.Request(
            url,
            data=None,
            headers={
                'User-Agent': 'Lynx/2.8.8dev.3 libwww-FM/2.14 SSL-MM/1.4.1'
            }
        )

        remote_data = urllib.request.urlopen(req)
    # print(remote_data.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        print('HTTPError: {0} Link {1} does not exist(s)'.format(e.code, url))
        return {}
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        print('URLError: {1} Reason: {0}'.format(e.reason, e.strerror))
        return {}
    else:
        # 200
        data = remote_data.read().decode('utf-8')
        json_data = json.loads(data)
        return json_data['movie_results'][0]


movies = 1000
page = 1
start = int(time.time())

for row in read_links(movies, page):
    credentials = "?api_key=1cd7d832933a3f8cb0c956ac70964e5f&language=en-US&external_source=imdb_id"
    link = "https://api.themoviedb.org/3/find/tt" + str(row[1]).zfill(7) + credentials
    if row[2] is not None:
        movie_data = fetch(link)

        if 'release_date' not in movie_data or len(movie_data['release_date']) == 0:
            (year, month, day) = (None, None, None)
            movie_data['release_date'] = None
        else:
            (year, month, day) = movie_data['release_date'].split('-')

        if movie_data['poster_path'] is None:
            poster = None
        else:
            poster = "https://image.tmdb.org/t/p/w780" + movie_data['poster_path']

        movie_query = "UPDATE movies.titles SET title = %s, original_title = %s, year = %s, release_date = %s, poster = %s, rating = %s, critics = %s, plot = %s, updated = %s WHERE title_id = %s"
        parameters = [movie_data['title'],
                      movie_data['original_title'],
                      year, movie_data['release_date'], poster,
                      movie_data['vote_count'], movie_data['vote_average'], movie_data['overview'],
                      int(time.time()),
                      row[0]
                      ]
        cur.execute(movie_query, parameters)
        conn.commit()
        # start of todo section
        # movie_query = "DELETE FROM movies.titles_genres WHERE title_id = %s"
        # parameters = [
        # 	row[0]
        # ]
        # end of section
        print("Updated", row[0], "with title", movie_data['original_title'],
              "(" + year + ") as tt" + str(row[1]).zfill(7))
# print(cur.query)

print("That whole thing lasted {} seconds".format(int(time.time()) - start))
