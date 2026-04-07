-- This Query Tab is where I will create and run all 4 queries

-- Question 1
Select flight_id, source_iata, dest_iata, travelDate
From Flights
Where source_iata = 'JFK' And dest_iata = 'PHX' And travelDate Between '2100-01-01' AND '2100-01-07'
Order by travelDate;

-- Question 2
Select f.source_iata as "Departing Flight" , f.dest_iata as "Destination Location", Count(*) AS total_bookings
From Reservation r
Join Flights f on r.flight_id = f.flight_id
Where f.travelDate Between '2100-01-01' AND '2100-01-07'
Group by f.source_iata, f.dest_iata
Order by total_bookings Desc
Limit 3;

-- Question 3
Select f.flight_id, f.source_iata, f.dest_iata, f.travelDate
From Flights f
Join Seats s on f.flight_id = s.flight_id
Where f.source_iata = 'JFK' And f.dest_iata = 'PHX' And s.is_available = TRUE
Group by f.flight_id, f.source_iata, f.dest_iata, f.travelDate
Order by f.travelDate ASC
Limit 1;

-- Question 4
Select AVG(occupancy_rate) as average_occupancy_rate
From(Select((Count(Case When s.is_available = False Then 1 END) / 300.0) * 100) as occupancy_rate
From Flights f
Join Seats s on f.flight_id = s.flight_id
Where f.source_iata = 'JFK' And f.dest_iata = 'LAX'
Group by f.flight_id) as flight_occupancy;




