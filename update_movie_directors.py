########################################################################################
# Reads theMovieDataBase field from movies.links and inserts Casting and Crew members
# todo: What should I do when movie does not exists in tmdb? (movie[2] is None)
########################################################################################
import random
import urllib.error
import urllib.request
import json
import psycopg2 as psy
import time

conn = psy.connect(host="192.168.1.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
cur = conn.cursor()


def get_all_movies():
    query = "SELECT l.*,t.title,t.year FROM movies.titles t, movies.links l WHERE l.title_id = t.title_id ORDER BY t.title_id LIMIT %s OFFSET %s;"
    cur.execute(query, [maximum, page * maximum])
    return cur.fetchall()


def fetch(internet):
    proxy_host = '192.168.1.9:3128'
    try:
        if random.randrange(10) % 2 == 0:
            browser = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:62.0) Gecko/20100101 Firefox/62.0'
        else:
            browser = 'Mozilla/4.0 (compatible; MSIE 5.0; Windows 98;)'
        req = urllib.request.Request(
            internet,
            data=None,
            headers={
                'User-Agent': browser
            }
        )
        req.set_proxy(proxy_host, 'http')

        remote_data = urllib.request.urlopen(req)
        data = remote_data.read().decode('utf-8')
        json_data = json.loads(data)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        print('HTTPError: {0} Link {1} does not exist(s)\n{2}'.format(e.code, link, movie))
        pass
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        print('URLError: {1} Reason: {0}\nInternet Link:\t{2}'.format(e.reason, e.strerror, link))
        pass
    else:
        # print('{}'.format(json_data))
        try:
            if json_data['status_code'] is 34:
                pass
        except KeyError:
            return json_data


def get_gender(gender):
    if gender == 2:
        return True
    if gender == 1:
        return False
    else:
        return None


def insert_people():
    query = "INSERT INTO movies.people VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, %s) ON CONFLICT (person_id) DO UPDATE SET updated = %s"
    personal['avatar'] = None
    if personal['profile_path'] is not None:
        personal['avatar'] = "https://image.tmdb.org/t/p/w780" + personal['profile_path']
    try:
        values = [
            personal['id'],
            personal['name'],
            personal['birthday'],
            personal['deathday'],
            get_gender(personal['gender']),
            personal['biography'],
            personal['place_of_birth'],
            personal['avatar'],
            personal['imdb_id'],
            now, int(time.time())
        ]
        cur.execute(query, values)
        conn.commit()
    except [psy.Error, psy.DatabaseError] as err:
        print('Line 84\tError: {0}'.format(err.pgerror))
        pass
    except KeyboardInterrupt:
        print('Line 87 Exception: Interrupted by User')
    else:
        pass


def insert_cast(member):
    # print('Starting Inserting Cast')
    # print(member)
    # print('*********************\n')
    query = "INSERT INTO movies.casting (title_id, cast_id, actor, character, male, credit_id, profile_img, appearance, person_id, updated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (title_id, cast_id, actor) DO UPDATE SET updated = %s;"
    member['avatar'] = None
    if member['profile_path'] is not None:
        member['avatar'] = "https://image.tmdb.org/t/p/w780" + member['profile_path']
    try:
        param = [
            movie[0],
            member['cast_id'],
            member['name'],
            member['character'],
            get_gender(member['gender']),
            member['credit_id'],
            member['avatar'],
            member['order'],
            member['id'],
            now, int(time.time())
        ]
        cur.execute(query, param)
        conn.commit()
    except [psy.Error, psy.DatabaseError] as err:
        print('Line 118\tError: {0}'.format(err.pgerror))
        pass
    else:
        #temp_line = '{2} ({3}) Cast {0} as {1}'.format(member['name'], member['character'], movie[3], movie[4])
        #print(temp_line + '\t\t\t ' * 2, end='\r')
        pass


def insert_crew():
    crew_sql = "INSERT INTO movies.crew VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (title_id,crew_id,credit) DO UPDATE SET updated = %s;"
    person_crew['avatar'] = None
    if person_crew['profile_path'] is not None:
        person_crew['avatar'] = "https://image.tmdb.org/t/p/w780" + person_crew['profile_path']
    try:
        parameters = [
            movie[0],
            person_crew['id'],
            person_crew['name'],
            get_gender(person_crew['gender']),
            person_crew['department'],
            person_crew['job'],
            person_crew['credit_id'],
            person_crew['avatar'],
            int(time.time())
        ]
        cur.execute(crew_sql, parameters)
        conn.commit()
    except [psy.Error, psy.DatabaseError] as err:
        print('Line 128\tError: {0}'.format(err.pgerror))
        pass
    else:
        #print('{2} ({3}) Crew {0} as {1}'.format(person_crew['name'], person_crew['job'], movie[3], movie[4]) )
        #temp_line = '{2} ({3}) Crew {0} of {1}\t{4}'.format(current + 1, maximum, movie[3], movie[4], person_crew['name'])
        #print(temp_line + '\t ' * 6, end='\r')
        pass


# Configuration segment
api_key = "1cd7d832933a3f8cb0c956ac70964e5f"
api_lang = "?api_key=" + api_key + "&language=en-US"
# 50 ταινίες είναι 90 minutes
# 100 movies equals 43 minutes
# 100 movies equals 52,5 minutes
# 100 movies equals 2221 seconds
# 100 movies equals 3099 seconds
# 100 movies equals 2361 seconds
# 100 movies equals 3397 seconds    -> sum =    , average =
# 1000 movies are 26586 seconds
# 1000 movies are 26418 seconds
# 1000 movies are 26450 seconds
# 1000 movies are 27446 seconds
# 1000 movies are 23547 seconds
# 1000 movies are 28802 seconds
# 1000 movies are 25796 seconds
# 1000 movies are 23717 seconds -> sum = 208762, average = 7h 14min 55,25 seconds
maximum = 1000
page = 46
# End of Configuration

now = int(time.time())
starting = now
current = 0
percent = 0

for movie in get_all_movies():
    link = "https://api.themoviedb.org/3/movie/" + str(movie[2]) + "/credits?api_key=" + api_key
    cast_info = fetch(link)
    movies_percent = 0
    try:
        for person_crew in cast_info['crew']:
            personal = fetch("https://api.themoviedb.org/3/person/" + str(person_crew['id']) + api_lang)
            insert_people()
            insert_crew()
        for people_cast in cast_info['cast']:
            personal = fetch("https://api.themoviedb.org/3/person/" + str(people_cast['id']) + api_lang)
            insert_people()
            insert_cast(people_cast)
        percent = 100 * float((current + 1) / maximum)
        movies_percent = 100 * float((page * maximum + current + 1) / 46092)
    except TypeError:
        print('[Line 198]\tError: {0} for Movie #{1}'.format(TypeError.args, movie[2]))
        pass
    current += 1
    # page * maximum + current
    line = 'Processed movie {0} of {1} ({3:.3f}%) or {2} or {5}\t{4:.6f}%'
    current_movie = page * maximum + current
    processed = line.format(str(current).zfill(len(str(maximum))), maximum, movie[2], percent, movies_percent,
                            current_movie)
    if current_movie % 100 == 0:
        print(processed + '\t ' * 6, end='\r\n')
    else:
        print(processed + '\t ' * 6, end='\r')

print('It took me {0} seconds in {1}'.format(int(time.time()) - starting, time.tzname[1]))
