# Import statements
import psycopg2
import psycopg2.extras
import csv
from config import *


# Write code / functions to set up database connection and cursor here.

def get_connection_and_cursor(): # NOTE - Code taken from section 11 in class assignment
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

conn, cur = get_connection_and_cursor()

def execute_and_print(query, numer_of_results=100):
    cur.execute(query)
    results = cur.fetchall()
    for r in results[:numer_of_results]:
        print(r)
    print('--> Result Rows:', len(results))
    print()


# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():

    # Create States table
    cur.execute("DROP TABLE IF EXISTS States CASCADE")
    cur.execute("DROP TABLE IF EXISTS Sites CASCADE")
    cur.execute("CREATE TABLE States(ID SERIAL PRIMARY KEY, Name VARCHAR(40) UNIQUE)")
    # Create Sites table
    cur.execute("""CREATE TABLE Sites(
    	ID SERIAL PRIMARY KEY,
    	Name VARCHAR(128) UNIQUE ,
    	Type VARCHAR(128),
    	Location VARCHAR(255),
    	Description Text,
    	State_ID INTEGER REFERENCES States (ID)) 
    	""")

    conn.commit()
    print('Setup database complete')


setup_database()
# Write code / functions to deal with CSV files and insert data into the database here.

class site(object):
	def __init__(self, park_list):
		self.name = park_list[0]
		self.location = park_list[1]
		self.type = park_list[2]
		self.address = park_list[3]
		self.description = park_list[4]


ahand = open('arkansas.csv','r')
areader = csv.reader(ahand)
chand = open('california.csv','r')
creader = csv.reader(chand)
mhand = open('michigan.csv','r')
mreader = csv.reader(mhand)
csvlist = [areader, creader, mreader]
statelist = ["AR","CA","MI"]
statedict = {'AR':1, 'CA': 2, 'MI':3}

for state in statelist:
	cur.execute(""" INSERT INTO States (Name) values (%s)""",(state,))
	conn.commit()

for statedata in csvlist:
	if statedata == areader:
		stateid = statedict['AR']
	elif statedata == creader:
		stateid = statedict['CA']
	elif statedata == mreader:
		stateid = statedict['MI']

	for row in statedata:
		if row[0] != 'NAME':
			site1 = site(row)
			# stateid = cur.execute('SELECT "ID" from "States" WHERE "Name" = "MI"')
			print(stateid)
			print(site1.name)

			cur.execute(""" INSERT INTO Sites (Name, Type, Location, Description, State_ID) values(%s, %s, %s, %s, %s)""",
			(site1.name, site1.type, site1.location, site1.description, stateid))

			conn.commit()
# Make sure to commit your database changes with .commit() on the database connection.
conn.commit()

# Write code to be invoked here (e.g. invoking any functions you wrote above)
# Write code to make queries and save data in variables here.

cur.execute("SELECT Sites.Location FROM Sites")
all_locations = cur.fetchall()

cur.execute("SELECT Sites.Name FROM Sites WHERE Sites.Description LIKE '%beautiful%' ")
beautiful_sites = cur.fetchall()

cur.execute("SELECT COUNT(Sites.Name) FROM Sites WHERE Sites.Type = 'National Lakeshore' ")
natl_lakeshores = cur.fetchall()

cur.execute("SELECT Sites.Name FROM Sites INNER JOIN States ON (Sites.State_ID = States.id) WHERE States.id = 3 ")
michigan_names = cur.fetchall()

cur.execute("SELECT COUNT(Sites.Name) FROM Sites WHERE Sites.Location Like '%AR%' ")
total_number_arkansas = cur.fetchall()

print (total_number_arkansas)

# We have not provided any tests, but you could write your own in this file or another file, if you want.
