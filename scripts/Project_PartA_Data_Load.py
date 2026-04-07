# Lets start with loading in the IATA Data

# Lets import the Pandas Library and Mysql Library
import mysql.connector
import pandas as pd

# Read the file
data = pd.read_csv('iata.txt', header= None, sep = ',')

# Lets set the columns that we need
data.columns = ['iata_code']

# Load the database
database = mysql.connector.connect(host='localhost', user='root', password ='Fast23**', database='Airline_Reservation_System')

# use cursor
mycursor = database.cursor()

# Populate the data into the database
for row in data.itertuples(index=False):
    mycursor.execute("""INSERT INTO airline_reservation_system.Airports(iata_code) VALUES (%s)""", (row.iata_code,))

# Commit changes to Database 
database.commit()