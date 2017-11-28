# Import statements
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from config import *
import sys
from csv import DictReader
from pprint import pprint

DEBUG = False

# Write code / functions to set up database connection and cursor here.
db_connection = None
db_cursor = None


def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            db_connection = psycopg2.connect(
                "dbname='{0}' user='{1}' password='{2}'".format(
                    db_name, db_user, db_password))
            print("Success connecting to database")
        except:
            print("Unable to connect to the database. "
                  "Check server and credentials.")
            sys.exit(1)  # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor


# Write code / functions to create tables with the columns you want
# and all database setup here.
def setup_database():
    conn, cur = get_connection_and_cursor()

    # Create States table
    if not DEBUG:
        cur.execute("""DROP TABLE IF EXISTS \"States\" CASCADE""")

    cur.execute("""CREATE TABLE IF NOT EXISTS \"States\" (
            \"ID\" SERIAL PRIMARY KEY,
            \"Name\" VARCHAR(40) UNIQUE NOT NULL)
        """)

    cur.execute("""DROP TABLE IF EXISTS \"Sites\"""")
    # Create Sites table
    cur.execute("""CREATE TABLE IF NOT EXISTS \"Sites\" (
        \"ID\" SERIAL PRIMARY KEY,
        \"Name\" VARCHAR(128) UNIQUE NOT NULL,
        \"Type\" VARCHAR(128),
        \"State_ID\" INTEGER NOT NULL,
        \"Location\" VARCHAR(255),
        \"Description\" TEXT)
    """)

    # Add foreign key constraint
    if not DEBUG:
        cur.execute("""ALTER TABLE \"Sites\"
            ADD CONSTRAINT \"Sites_FK_States\"
            FOREIGN KEY (\"State_ID\")
            REFERENCES \"States\" (\"ID\")
            ON DELETE NO ACTION ON UPDATE NO ACTION
        """)

    conn.commit()

    print('Setup database complete')


# Write code / functions to deal with CSV files and insert data
# into the database here.
def load_data_from_csv(file_name):
    data = []
    try:
        with open(file_name, 'r', encoding='utf-8') as csvfile:
            rowreader = DictReader(csvfile)
            for row in rowreader:
                data.append(row)
    except FileNotFoundError:
        data = None

    return data


def insert_states():
    conn, cur = get_connection_and_cursor()

    cur.execute("""INSERT INTO \"States\" (\"ID\", \"Name\")
        VALUES (1, \'Arkansas\')
        ON CONFLICT DO NOTHING;
        INSERT INTO \"States\" (\"ID\", \"Name\")
        VALUES (2, \'California\')
        ON CONFLICT DO NOTHING;
        INSERT INTO \"States\" (\"ID\", \"Name\")
        VALUES (3, \'Michigan\')
        ON CONFLICT DO NOTHING;
        """)

    conn.commit()

    print('Complete inserting states')


def insert_sites(state_file_name, state_num):
    conn, cur = get_connection_and_cursor()

    data_dict = load_data_from_csv(state_file_name)
    for row_site in data_dict:
        # generate insert into query string
        query = sql.SQL("""INSERT INTO \"Sites\" (
            \"Name\", \"Type\",
            \"State_ID\", \"Location\", \"Description\")
            VALUES({0}, {1}, {4}, {2}, {3}) ON CONFLICT DO 
            NOTHING""").format(
            sql.SQL("\'" + row_site['NAME'].replace("\'", "\'\'") + "\'"),
            sql.SQL("\'" + row_site['TYPE'].replace("\'", "\'\'") + "\'"),
            sql.SQL("\'" + row_site['LOCATION'].replace("\'", "\'\'") + "\'"),
            sql.SQL(
                "\'" + row_site['DESCRIPTION'].replace("\'", "\'\'") + "\'"),
            sql.SQL(str(state_num))
        )
        query_string = query.as_string(conn)
        cur.execute(query_string)

    conn.commit()

    print('Complete inserting sites in {0}'.format(state_file_name))


# Make sure to commit your database changes with .commit()
# on the database connection.


# Write code to be invoked here (e.g. invoking any functions you wrote above)
setup_database()

insert_states()
insert_sites("arkansas.csv", 1)
insert_sites("california.csv", 2)
insert_sites("michigan.csv", 3)


# Write code to make queries and save data in variables here.
conn, cur = get_connection_and_cursor()


cur.execute("""SELECT \"Location\" FROM \"Sites\"""")
all_locations = cur.fetchall()

cur.execute("""SELECT \"Name\" FROM \"Sites\"
    WHERE \"Description\" LIKE \'%beautiful%\'
""")
beautiful_sites = cur.fetchall()

cur.execute("""SELECT COUNT(*) FROM \"Sites\"
    WHERE \"Type\"=\'National Lakeshore\'
""")
natl_lakeshores = cur.fetchall()

cur.execute("""SELECT \"Sites\".\"Name\" FROM \"Sites\"
    INNER JOIN \"States\"
    ON \"Sites\".\"State_ID\"=\"States\".\"ID\"
    WHERE \"States\".\"Name\"=\'Michigan\'
""")
michigan_names = cur.fetchall()

cur.execute("""SELECT COUNT(*) FROM \"Sites\"
    INNER JOIN \"States\"
    ON \"Sites\".\"State_ID\"=\"States\".\"ID\"
    WHERE \"States\".\"Name\"=\'Arkansas\'
""")
total_number_arkansas = cur.fetchall()

# pprint(all_locations)
# pprint(beautiful_sites)
# pprint(natl_lakeshores)
# pprint(michigan_names)
# pprint(total_number_arkansas)

conn.commit()


# We have not provided any tests, but you could write your own in this file
# or another file, if you want.
