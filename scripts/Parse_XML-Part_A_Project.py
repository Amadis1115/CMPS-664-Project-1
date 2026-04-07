# Now lets work on parsing the XMl File

# Import libraries we need
import mysql.connector
import xml.etree.ElementTree as ET

tree = ET.parse("PNR.xml")
root = tree.getroot()

# First we have to strip the namespaces
for element in root.iter():
    if "}" in element.tag:
        element.tag = element.tag.split("}", 1)[1]

# Intialize list to store rows
rows = []
# Loop through rows
for row in root.iter("Row"):
    #initialize a list for values
    values = []
    # Loop though cells
    for cell in row.iter("Cell"):
        data = cell.find("Data")
        values.append(data.text if data is not None else None)
    if values:
        rows.append(values)

# initialize headers
headers = rows[0]
# Create a dictionary with the records
records = [dict(zip(headers, row)) for row in rows[1:] if len(row) == len(headers)]

# Load the database
database = mysql.connector.connect(host='localhost', user='root', password ='Fast23**', database='Airline_Reservation_System')

# use cursor
mycursor = database.cursor()

# Lets populate the Passenger table
#Loop through our record dictionary
for record in records:
    firstname = record['firstname']
    lastname = record['lastname']
    address = record['address']
    age = int(record['age'])

    # Insert into the table
    mycursor.execute("""INSERT INTO Passenger (firstname, lastname, age, address) VALUES (%s, %s, %s, %s)""", (firstname, lastname, age, address))
    passenger_id = mycursor.lastrowid

# Lets populate the Flight table
    source = record['source']
    dest = record['dest']
    travelDate = record['travelDate']

# In order to avoid duplicates we need to make sure that the flight doesnt already exist
    mycursor.execute("""SELECT flight_id FROM Flights WHERE source_iata = %s AND dest_iata = %s AND travelDate = %s""", (source, dest, travelDate))

# Fetch the next row
    result = mycursor.fetchone()

# If the flight exists..else
    if result:
        flight_id = result[0]
    else:
        mycursor.execute(""" INSERT INTO Flights (source_iata, dest_iata, travelDate) VALUES (%s, %s, %s) """, (source, dest, travelDate))
# Return value for our auto incremental flight_id column
        flight_id = mycursor.lastrowid

# Now I will work on the class mapping since we need to convert them from text to id in order to make the insertion of data easier
    class_map = {'economy' : 1, 'business' : 2, 'first' : 3}

# Now we will need to populate the Reservation table
    booking_time = record['bookingTime']
    npass = int(record['npass'])
    requested_class_id = class_map[record['class'].lower()]

    mycursor.execute("""INSERT INTO Reservation (booking_time, passenger_id, flight_id, requested_class_id, n_pass) VALUES (%s, %s, %s, %s, %s)""", (booking_time, passenger_id, flight_id, requested_class_id, npass))

database.commit()


# Now I work on the populating the Seats table
mycursor.execute("SELECT flight_id FROM Flights")
# initialize the flight variable with all of the flight ID's
flights = mycursor.fetchall()

# Loop through the flights
for (flight_id,) in flights:
    # There are 50 seats in First class
    for i in range(1,51):
        seat_number = f"F{i}"
        mycursor.execute("""INSERT INTO Seats(flight_id, seat_number, class_id, is_available) VALUES (%s, %s, %s, %s)""", (flight_id, seat_number, 3, True))

    # There are 100 seats in business class so we will do the same process
    for i in range(1,101):
        seat_number = f"B{i}"
        mycursor.execute("""INSERT INTO Seats(flight_id, seat_number, class_id, is_available) VALUES (%s, %s, %s, %s)""", (flight_id, seat_number, 2, True))
    
    # There are 150 seats in Economy class so I will do the same thing to this class
    for i in range(1,151):
        seat_number = f"E{i}"
        mycursor.execute("""INSERT INTO Seats(flight_id, seat_number, class_id, is_available) VALUES (%s, %s, %s, %s)""", (flight_id, seat_number, 1, True))
    
database.commit()


# Now Im going to work on the reserve seat logic
def reserve_seat(reservation_id, mycursor, database):
    # First we have to check if the seat is already assigned
    mycursor.execute("""SELECT COUNT(*) FROM Reservation_Seat WHERE reservation_id = %s""",(reservation_id,))
    
    # Fetch all of the seats already assigned
    already_assigned = mycursor.fetchone()[0]

    if already_assigned > 0:
        print(f"Reservation {reservation_id} already has seats assigned.")
        return
    
    # Get the reservation details
    mycursor.execute(""" SELECT flight_id, requested_class_id, n_pass FROM Reservation WHERE reservation_id = %s""", (reservation_id,))

    # Fetch reservations
    reservation = mycursor.fetchone()
    
    # If reservation not found 
    if not reservation:
        print(f"Reservation {reservation_id} not found.")
        return
    
    # Intialize variables with reservationd details
    flight_id, requested_class_id, n_pass = reservation
    seats_needed = n_pass
    assigned_seats = []

    # Now lets work on the priority order rule
    if requested_class_id == 1:
        class_order = [1, 2, 3]
    elif requested_class_id == 2:
        class_order = [2, 3, 1]
    elif requested_class_id == 3:
        class_order = [3, 2, 1]
    else:
        print("Invalid class ID.")
        return
    
    
    # Lets find the seats
    for class_id in class_order:
        if seats_needed == 0:
            break

        #Query through the Seats table to find available seats
        mycursor.execute(""" SELECT seat_id FROM Seats WHERE flight_id = %s AND class_id = %s AND is_available = TRUE LIMIT %s""", (flight_id, class_id, seats_needed))
        # Fetch through the available seats
        available_seats = mycursor.fetchall()

        # Loop through available seats
        for (seat_id,) in available_seats:
            assigned_seats.append(seat_id)
            seats_needed -= 1
            if seats_needed == 0:
                break

    if seats_needed > 0:
        print(f" Not enough seats available for reservation {reservation_id}.")
        return
        
    # Now Lets save the seat assignments
    for seat_id in assigned_seats:
        mycursor.execute(""" INSERT INTO Reservation_Seat (reservation_id, seat_id) VALUES (%s, %s)""", (reservation_id, seat_id))

        mycursor.execute(""" UPDATE Seats SET is_available = FALSE WHERE seat_id = %s """, (seat_id,))

    database.commit()

# Now I will work on the first come first served logic
mycursor.execute("SELECT reservation_id FROM Reservation ORDER By booking_time ASC")
reservations = mycursor.fetchall()

    # Loop through Reservations again
for (reservation_id,) in reservations:
    reserve_seat(reservation_id, mycursor, database)

# Create the Checkin logic

# Import libraries needed for this
from datetime import datetime, timedelta

def check_in(reservation_id, checkin_time, mycursor, database):
    # Check if already checked in
    mycursor.execute("""
        SELECT COUNT(*) 
        FROM CheckIn 
        WHERE reservation_id = %s
    """, (reservation_id,))
    
    already_checked_in = mycursor.fetchone()[0]
    
    if already_checked_in > 0:
        print(f"Reservation {reservation_id} has already checked in.")
        return

    # Get the flight departure date
    mycursor.execute("""
        SELECT f.travelDate
        FROM Reservation r
        JOIN Flights f ON r.flight_id = f.flight_id
        WHERE r.reservation_id = %s
    """, (reservation_id,))
    
    result = mycursor.fetchone()
    
    if not result:
        print(f"Reservation {reservation_id} not found.")
        return

    departure_datetime = result[0]

    # Convert checkin_time to datetime if needed
    if isinstance(checkin_time, str):
        checkin_time = datetime.strptime(checkin_time, "%Y-%m-%d %H:%M:%S")

    # Check rules
    if checkin_time > departure_datetime:
        print(f"Check-in failed: flight already departed for reservation {reservation_id}")
        return

    if checkin_time < departure_datetime - timedelta(hours=24):
        print(f"Check-in failed: too early to check in for reservation {reservation_id}")
        return

    # Insert check-in record
    mycursor.execute("""
        INSERT INTO CheckIn (reservation_id, checkin_time)
        VALUES (%s, %s)
    """, (reservation_id, checkin_time))

    database.commit()
    print(f"Check-in successful for reservation {reservation_id}")
    
print('Data Loaded to MYSQL Correctly!')