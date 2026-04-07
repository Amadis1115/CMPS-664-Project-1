DROP DATABASE IF EXISTS Airline_Reservation_System;
Create Database Airline_Reservation_System;
use Airline_Reservation_System;


Create Table Passenger (
passenger_id int unsigned NOT NULL auto_increment PRIMARY KEY,
firstname varchar(100) NOT NULL,
lastname varchar(100) NOT NULL,
age int NOT NULL,
address varchar(100) NOT NULL);

Create Table Class (
class_id INT unsigned Primary Key,
class_name Varchar(20) NOT NULL);

Create Table Airports (
iata_code Varchar(10) Primary Key);


Create Table Flights (
flight_id int unsigned NOT NULL auto_increment PRIMARY KEY,
source_iata varchar(10) NOT NULL,
dest_iata varchar(10) NOT NULL,
travelDATE datetime NOT NULL,
FOREIGN KEY (source_iata) References Airports(iata_code),
FOREIGN KEY (dest_iata) References Airports(iata_code));


Create Table Reservation(
reservation_id int unsigned NOT NULL auto_increment PRIMARY KEY,
booking_time time NOT NULL,
passenger_id int unsigned NOT NULL,
flight_id int unsigned NOT NULL,
requested_class_id int unsigned NOT NULL,
n_pass int NOT NULL,
FOREIGN KEY (passenger_id) REFERENCES Passenger(passenger_id),
FOREIGN KEY (flight_id) REFERENCES Flights(flight_id),
FOREIGN KEY (requested_class_id) REFERENCES Class(class_id));


Create Table Seats(
seat_id int unsigned NOT NULL auto_increment PRIMARY KEY,
flight_id int unsigned NOT NULL,
seat_number varchar(10) NOT NULL,
class_id int unsigned NOT NULL,
is_available Boolean NOT NULL,
FOREIGN KEY (flight_id) REFERENCES Flights(flight_id),
FOREIGN KEY (class_id) REFERENCES Class(class_id));

Create Table CheckIn(
checkin_id int unsigned NOT NUll auto_increment PRIMARY KEY,
reservation_id int unsigned NOT NULL,
checkin_time timestamp NOT NULL,
FOREIGN KEY (reservation_id) REFERENCES Reservation(reservation_id));

Create Table Seat_Reservation(
reservation_id INT unsigned NOT NULL,
seat_id INT unsigned NOT NULL,
PRIMARY KEY(reservation_id, seat_id),
FOREIGN KEY (reservation_id) REFERENCES Reservation(reservation_id),
FOREIGN KEY(seat_id) REFERENCES Seats(seat_id));


INSERT INTO Class (class_id, class_name) Values
(1, 'economy'),
(2, 'business'),
(3, 'first');










