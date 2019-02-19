import requests
import json
import sqlite3
from datetime import datetime
import keys


def date(datestr, format = "%Y-%m-%d %H:%M:%S"):                                # a function to print out the date/time
                                                                                # of the event in a nice way
    d = datetime.strptime(datestr, format)
    pretty = d.strftime("%a, %b %d, %Y at %H:%M")
    return pretty



ticketmaster_key = keys.ticketmaster_key                                        #assign ticketmaster key from keys file

CACHE_FNAME = "ticketmaster_cache.json"                                         #create a cache for ticketmaster events
try:
	cache_file = open(CACHE_FNAME, "r")					                        #try to read file
	cache_contents = cache_file.read()					                        #if it's there, read it as a string
	CACHE_DICTION = json.loads(cache_contents)			                        #load it as a dictionary
	cache_file.close()									                        #close file
except:
	CACHE_DICTION = {}



def get_event_info(search, ticketmaster_key = ticketmaster_key):                #function to get event info from ticketmaster
                                                                                #it either gets from the cache if it's there or
    if search in CACHE_DICTION:                                                 #requests from the API
        d = CACHE_DICTION[search]
    else:                                                                       #the params for the URL include the search, key,
        data = requests.get("https://app.ticketmaster.com/discovery/v2/events", #format, dmaId which is the New York code, size of response
            params = {"keyword": search, "apikey": ticketmaster_key,
            "format":"json", "dmaId": "345", "size": 100})

        d = json.loads(data.text)
        CACHE_DICTION[search] = d
        f = open(CACHE_FNAME, 'w')
        f.write(json.dumps(CACHE_DICTION))
        f.close()

    return d


ticket_search = get_event_info("music")                                         #search Ticketmaster for "music"


conn = sqlite3.connect('ticketmaster.sqlite')			                        #create a database
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS Events')			                           	#if the table Events exists, clear the info and update it
cur.execute('''
CREATE TABLE Events (event_id TEXT, name TEXT, event_date TEXT, venue_id TEXT,
    category TEXT)''')

cur.execute('DROP TABLE IF EXISTS Venues')				                        #if the table Venues exists, clear the info and update it
cur.execute('''
CREATE TABLE Venues (event_name TEXT, venue_name TEXT, address TEXT,
    longitude TEXT, latitude TEXT)''')

other_categories = []                                                           #create a list for categories

for event in ticket_search["_embedded"]["events"]:                              #for each event gather the following data
	a = event["id"]
	b = event["name"]
	if "dateTime" in event["dates"]["start"]:
		c = event["dates"]["start"]["dateTime"].replace("T", " ").replace("Z", "")
	else:
		c = "NONE"

	if "classifications" in event:
		i = event["classifications"][0]["segment"]["name"]
		if "genre" in event["classifications"][0]:
			if event["classifications"][0]["genre"]["name"] != "Undefined":
				other_categories.append(event["classifications"][0]["genre"]["name"])
		if "subGenre" in event["classifications"][0]:
			if event["classifications"][0]["subGenre"]["name"] != "Undefined":
				other_categories.append(event["classifications"][0]["subGenre"]["name"])
		if "type" in event["classifications"][0]:
			if event["classifications"][0]["type"]["name"] != "Undefined":
				other_categories.append(event["classifications"][0]["type"]["name"])
		if "subType" in event["classifications"][0]:
			if event["classifications"][0]["subType"]["name"] != "Undefined":
				other_categories.append(event["classifications"][0]["subType"]["name"])
	else:
		i = "NONE"

	if "_embedded" in event:
		for venue in event["_embedded"]["venues"]:
			d = venue["name"]
			e = ""
			if "address" in venue:
				for line in venue["address"]:
					e += venue["address"][line]
			e += ", " + (venue["city"]["name"]) + ", "
			if "state" in venue:
				if "stateCode" in venue["state"]:
					e += (venue["state"]["stateCode"]) + " "
			e += (venue["postalCode"])
			if "location" in venue:
				f = venue["location"]["longitude"]
				g = venue["location"]["latitude"]
			else:
				f = "NONE"
				g = "NONE"
			h = venue["id"]

                                                                                #add the data to the datebases accordingly
	cur.execute('''INSERT INTO Events (event_id, name, event_date, venue_id, category)
            VALUES (?, ?, ?, ?, ?)''', (a, b, c, h, i))

	cur.execute('''INSERT INTO Venues (event_name, venue_name, address, longitude,
        latitude) VALUES (?, ?, ?, ?, ?)''', (b, d, e, f, g))

	conn.commit()


                                                                                #print out the event information gathered
                                                                                #from ticketmaster
print ("\n\nCheck out these events taking place on New Years Eve in New York City!!!\nvia Ticketmaster\n\n")


sqlstr = "SELECT name, event_date FROM Events"
for row in cur.execute(sqlstr):
    print (str(row[0]) + " on " + str(date(row[1])) + "\n")                     #print the name and time of the event,
                                                                                #the time prints as Sat, Dec 9, 2017 by running
                                                                                #date function


m = open("map_coordinates.csv", "w")                                            #create/open csv file for coordinates
m.write("Venue, latitude, longitude\n")                                         #create the headers
sqlstr = "SELECT venue_name, longitude, latitude FROM Venues"
for row in cur.execute(sqlstr):
	m.write("{}, {}, {}\n".format(row[0], row[2], row[1]))                      #for each row, insert the name, latitude, and longitude




eventbrite_token = keys.eventbrite_token                                        #assign eventbrite key from keys file

CACHE_FNAME = "eventbrite_cache.json"                                           #create cache for eventbrite events
try:
	cache_file = open(CACHE_FNAME, "r")					                        #try to read file
	cache_contents = cache_file.read()					                        #if it's there, read it as a string
	CACHE_DICTION = json.loads(cache_contents)		                           	#load it as a dictionary
	cache_file.close()									                        #close file
except:
	CACHE_DICTION = {}


def event_info(search, eventbrite_token = eventbrite_token):                    #function to retrieve data from Eventbrite

    if search in CACHE_DICTION:
        d = CACHE_DICTION[search]
    else:                                                                       #params are search, token, format, New York
        data = requests.get("https://www.eventbriteapi.com/v3/events/search/?",
            params = {"q": search, "token":eventbrite_token, "format":"json",
            "location.address": "New York, NY" })
        d = json.loads(data.text)
        CACHE_DICTION[search] = d
        f = open(CACHE_FNAME, 'w')
        f.write(json.dumps(CACHE_DICTION))
        f.close()

    return d


venue_cache = "eb_venue_cache.json"                                             #crate a cache for Eventbrite venue info since a different type of response
try:
	venue_file = open(venue_cache, "r")					                        #try to read file
	venue_contents = venue_file.read()					                        #if it's there, read it as a string
	venue_DICTION = json.loads(venue_contents)			                        #load it as a dictionary
	venue_file.close()									                        #close file
except:
	venue_DICTION = {}


def get_venue_info(venue_id, eventbrite_token = eventbrite_token):              #function to retrieve data about venues using venue id

	if venue_id in venue_DICTION:
		v = venue_DICTION[venue_id]
	else:
		venue_data = requests.get("https://www.eventbriteapi.com/v3/venues/{0}/?".format(venue_id),
            params = {"token":eventbrite_token, "format":"json"})
		v = json.loads(venue_data.text)
		venue_DICTION[venue_id] = v
		f = open(venue_cache, 'w')
		f.write(json.dumps(venue_DICTION))
		f.close()

	return v

                                                                                #create cache for Eventbrite category info
category_cache = "eb_category_cache.json"
try:
	cat_file = open(category_cache, "r")					                    #try to read file
	cat_contents = cat_file.read()					                            #if it's there, read it as a string
	cat_DICTION = json.loads(cat_contents)			                            #load it as a dictionary
	cat_file.close()									                        #close file
except:
	cat_DICTION = {}

                                                                                #function to retrieve info about categories
def get_category(cat_id, eventbrite_token = eventbrite_token):

	if cat_id in cat_DICTION:
		c = cat_DICTION[cat_id]
	else:
		cat_data = requests.get("https://www.eventbriteapi.com/v3/categories/{0}/?".format(cat_id),
            params = {"token":eventbrite_token, "format":"json"})
		c = json.loads(cat_data.text)
		cat_DICTION[cat_id] = c
		f = open(category_cache, 'w')
		f.write(json.dumps(cat_DICTION))
		f.close()

	return c



conn = sqlite3.connect('eventbrite.sqlite')			                            #create a database for Eventbrite
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS Events')				                        #if the table Events exists, clear the info and update it
cur.execute('''
CREATE TABLE Events (event_id TEXT, name TEXT, start_time INTEGER, venue_id TEXT, category TEXT)''')

cur.execute('DROP TABLE IF EXISTS Venues')				                        #if the table Venues exists, clear the info and update it
cur.execute('''
CREATE TABLE Venues (event_name TEXT, venue_name TEXT, address TEXT, longitude TEXT, latitude TEXT)''')

                                                                                #search Eventbrite for "music" and gather the following information
event_search = event_info("music")

for event in event_search["events"]:
	a = event["id"]
	b = event["name"]["text"]
	c = event["start"]["local"].replace("T", " ")
	e = event["venue_id"]

	venue_search = get_venue_info(e)

	f = venue_search["name"]
	g = (venue_search["address"]["localized_address_display"])
	h = venue_search["longitude"]
	i = venue_search["latitude"]

	if event["category_id"]:
		get_cat = get_category(event["category_id"])
		j = get_cat["name"]

                                                                                #store the data in the database in respective columns
	cur.execute('''INSERT INTO Events (event_id, name, start_time, venue_id, category)
            VALUES (?, ?, ?, ?, ?)''', (a, b, c, e, j))

	cur.execute('''INSERT INTO Venues (event_name, venue_name, address, longitude, latitude)
			VALUES (?, ?, ?, ?, ?)''', (b, f, g, h, i))

	conn.commit()


                                                                                #print out the info for Eventbrite events
print ("\n\nCheck out these events taking place on New Years Eve in New York City!!!\nvia Eventbrite\n\n")


sqlstr = "SELECT name, start_time FROM Events"
for row in cur.execute(sqlstr):
	print (str(row[0]) + " on " + str(date(row[1])) + "\n")                     #print the name and time of the event,
                                                                                #the time prints as Sat, Dec 9, 2017 by running
                                                                                #date function


sqlstr = "SELECT venue_name, longitude, latitude FROM Venues"                   #write the geodata to the csv coordinates file
for row in cur.execute(sqlstr):
	m.write("{}, {}, {}\n".format(row[0], row[2], row[1]))



m.close()                                                                       #close csv file
cur.close()                                                                     #close cursor
