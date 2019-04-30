#!/usr/bin/python
import psycopg2
 
 
def write_blob(id, path_to_file,img_name, img_type ):
    """ insert a BLOB into a table """
    conn = None
    try:
        # read data from a picture
        img = open(path_to_file, 'rb').read()
        # connect to the PostgresQL database
        conn = psycopg2.connect(host="localhost",database="postgres", user="denishpatel", password="postgres")
        # create a new cursor object
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute("INSERT INTO images(id,img_name,img_type,img) " +
                    "VALUES(%s,%s,%s,%s)",
                    (id, img_name, img_type, psycopg2.Binary(img)))
        # commit the changes to the database
        conn.commit()
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def read_blob(id, path_to_dir):
    """ read BLOB data from a table """
    conn = None
    try:
        # connect to the PostgresQL database
        conn = psycopg2.connect(host="localhost",database="postgres", user="denishpatel", password="postgres")
        # create a new cursor object
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(""" SELECT img_name,img_type, img from images 
                        WHERE id = %s """,
                    (id,))
 
        blob = cur.fetchone()
        open(path_to_dir  + blob[0] + '.' + blob[1], 'wb').write(blob[2])
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    write_blob(1, 'images/postgres_elephant.jpg','postgres_elephant', 'jpg')
    print "image is written to database"
    read_blob (1, 'out/')
    print "image is read from database and put into ./out/ folder"
