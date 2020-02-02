import mysql.connector as maria_database

# connection = psy.connect(host="192.168.1.13", database="aluminium", user="ntina23gr", password="!p9lastiras")
# cursor = connection.cursor()
maria_connection = maria_database.connect(host='192.168.1.6', user='ntina23gr', password='!sm3llyc4t',
                                          database='movies_pro')
cursor = maria_connection.cursor()
# tell postgres to use more work memory
work_mem = 2048


def main():
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    print("Connected into Database!")

    # execute our Query
    cursor.execute("SELECT * FROM movies.credentials LIMIT 25")

    # retrieve the records from the database
    records = cursor.fetchall()

    for record_index in range(len(records)):
        if record_index + 1 != records[record_index][0]:
            # print(record_index + 1, "is not", records[record_index][0])
            update_query = "UPDATE movies.userinfo SET user_id = %s WHERE user_id = %s;"
            cursor.execute(update_query, [record_index + 1, records[record_index][0]])
            empty = records[record_index][0] - (record_index + 1)
            print(cursor.mogrify(update_query, [record_index + 1, records[record_index][0]]), empty, end='\r')
            maria_connection.commit()
        if record_index % 100 == 0:
            print(record_index, "\t {0:.5f} %".format(100 * record_index / len(records)), end='\r')


if __name__ == "__main__":
    main()
    print("Have updated some rows in table")
