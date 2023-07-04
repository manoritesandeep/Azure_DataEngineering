#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[6]:


# TO-DO: Create a Keyspace 
try:
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS project_udacity
        WITH REPLICATION = 
            {'class': 'SimpleStrategy', 'replication_factor': 1} """
                   )
except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('project_udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# 
#     0 artist
#     1 firstName of user
#     2 gender of user
#     3 item number in session
#     4 last name of user
#     5 length of the song
#     6 level (paid or free song)
#     7 location of the user
#     8 sessionId
#     9 song title
#     10 userId
# 
# 
# ['Stephen Lynch', 'Logged In', 'Jayden', 'M', '0', 'Bell', '182.85669', 'free', 'Dallas-Fort Worth-Arlington, TX', 'PUT', 'NextSong', '1.54099E+12', '829', "Jim Henson's Dead", '200', '1.54354E+12', '91'], 
# 
# ['Manowar', 'Logged In', 'Jacob', 'M', '0', 'Klein', '247.562', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'Shell Shock', '200', '1.54354E+12', '73']

# In[8]:


# session.execute("DROP TABLE song_library;")


# In[9]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
query = "CREATE TABLE IF NOT EXISTS song_library"
query = query + "(artist text, song_title text, song_length DOUBLE, sessionId INT, itemInSession INT,                 PRIMARY KEY (sessionId, itemInSession))"

try:
    session.execute(query)
except Exception as e:
    print(e)                    


# In[10]:


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
#         print(f"line 0: {line[0]}")
#         print(f"line 9: {line[9]}")
#         print(f"line 6: {line[5]}")
#         print(f"line 9: {line[8]}")
#         print(f"line 4: {line[3]}")
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO song_library (artist, song_title, song_length, sessionId, itemInSession)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[0], line[9], float(line[5]), int(line[8]), int(line[3])))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[11]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "SELECT artist, song_length, song_title FROM song_library WHERE sessionId = 338 AND itemInSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for r in rows:
#     print(len(r))
#     print(r) # Row(artist='Faithless', song_length=495.3073, song_title='Music Matters (Mark Knight Dub)')
    print(f"Artist: {r.artist}, Song Title: {r.song_title}, Song length: {r.song_length}")


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[12]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

# Create table
query = "CREATE TABLE IF NOT EXISTS query2_table"
query = query + "(artist text, song_title text, first_name text, last_name text, userid INT,                 sessionId INT, itemInSession INT,                 PRIMARY KEY (userid, sessionId))"

try:
    session.execute(query)
except Exception as e:
    print(e)                    


# In[13]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
#         print(f"line 0: {line[0]}")
#         print(f"line 9: {line[9]}")
#         print(f"line 6: {line[5]}")
#         print(f"line 9: {line[8]}")
#         print(f"line 4: {line[3]}")
#         print(f"line 6: {line[1]}")
#         print(f"line 9: {line[4]}")
#         print(f"line 4: {line[10]}")
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO query2_table (artist, song_title, first_name, last_name, userid, sessionId, itemInSession)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[0], line[9], line[1], line[4], int(line[10]), int(line[8]), int(line[3])))


# In[14]:


# Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

query = "SELECT artist, song_title, first_name, last_name             FROM query2_table             WHERE userid = 10 AND sessionId = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for r in rows:
#     print(r)
    print(f"Name of Artist: {r.artist}, Song Title: {r.song_title}.")


# In[15]:


# session.execute("DROP TABLE query3_table")


# In[16]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# Create table 
query = "CREATE TABLE IF NOT EXISTS query3_table"
query = query + "(first_name TEXT, last_name TEXT, song_title TEXT, PRIMARY KEY (song_title))"

try:
    session.execute(query)
except Exception as e:
    print(e)                    


# In[17]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO query3_table (first_name, last_name, song_title)"
        query = query + "VALUES (%s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (line[1], line[4], line[9]))


# In[18]:


# Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "SELECT first_name, last_name          FROM query3_table          WHERE song_title = 'All Hands Against His Own'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for r in rows:
    print(f"First Name:{r.first_name}, Last Name: {r.last_name}")


# ### Drop the tables before closing out the sessions

# In[19]:


## TO-DO: Drop the table before closing out the sessions

query = "drop table song_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
query1 = "drop table query2_table"
try:
    rows = session.execute(query1)
except Exception as e:
    print(e)
        
query2 = "drop table query3_table"
try:
    rows = session.execute(query2)
except Exception as e:
    print(e)


# In[ ]:





# ### Close the session and cluster connection¶

# In[20]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




