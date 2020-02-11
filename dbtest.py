import psycopg2
import json


r = {'is_claimed': 'True', 'rating': 3.5}
r = json.dumps(r)

try:
    connection = psycopg2.connect(user = "andras2",
                                  password = "",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "phoebe")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")


    cursor = connection.cursor()
    
    postgres_insert_query = """ INSERT INTO images (url,hash,exif,size) VALUES (%s,%s,%s,%s)"""
    record_to_insert = ('url', 'hash', r, 999)
    cursor.execute(postgres_insert_query, record_to_insert)

    connection.commit()
    count = cursor.rowcount
    print (count, "Record inserted successfully into mobile table")


except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
