# Reads count(genre_id) that user rated most and updates user info table accordingly
import psycopg2 as psy

database_host = "192.168.1.13"
database_name = 'aluminium'
database_user = "ntina23gr"
database_pass = "!p9lastiras"


connection = None
limited = 343961


def main():
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = connection.cursor()
    users = get_all_users(cursor, limited)
    for user in users:
        get_user_ratings(cursor, user)


def connect():
    try:
        # Define our connection, if a connect cannot be made an exception will be raised here
        return psy.connect(host=database_host, database=database_name, user=database_user, password=database_pass)
    except psy.DatabaseError as connect_error:
        print("I am unable to connect to the database\n{}".format(connect_error))


def set_user_genre(cursor, user, genre):
    # print("Setting genre {} for user {}".format(genre, user))
    query = "UPDATE movies.userinfo SET fav_genre = %s WHERE user_id = %s"
    try:
        cursor.execute(query, [genre, user])
        # print(cursor.mogrify(query, [genre, user]))
        connection.commit()
    except psy.DatabaseError as database_error:
        print("[Line 36]\t{}".format(database_error))
    pass


def get_user_ratings(cursor, user):
    query = "SELECT genre_id,count(genre_id) FROM movies.titles_genres WHERE title_id IN (SELECT title_id FROM movies.ratings WHERE user_id = %s) GROUP BY genre_id ORDER BY count(genre_id) DESC"
    try:
        cursor.execute(query, [user[0]])
        rows = cursor.fetchall()
        # print("Showing genres for user {}".format(user[0]))
        # for row in rows:
        #     print("Row {} for user {}".format(row, user[0]))
        # print("******************\n{}\n*****************".format(rows[0][0]))
        print("Setting genre {}\tfor {} {} <{}>".format(rows[0][0], user[1], user[2], user[3]))
        set_user_genre(cursor, user[0], rows[0][0])
        return rows
    except psy.Error as error:
        print("Line 35 Exception: {}".format(error))
    except IndexError as idx_error:
        # print("IndexError: {}\nSkipping ratings for user {}".format(idx_error, user[0]))
        pass


def get_all_users(cursor, many):
    page = 0
    query = "SELECT u.user_id,u.forename,u.lastname,c.usermail FROM movies.userinfo u, movies.credentials c WHERE c.username=u.username ORDER BY user_id OFFSET %s LIMIT %s"
    try:
        cursor.execute(query, [page * many, many])
        return cursor.fetchall()
    except psy.Error as error:
        print("[Line 50]\t{}".format(error))


if __name__ == "__main__":
    connection = connect()
    main()
