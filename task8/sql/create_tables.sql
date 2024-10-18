CREATE TABLE IF NOT EXISTS airline_raw_data (
    Number INT,
    Passenger_ID STRING,
    First_Name STRING,
    Last_Name STRING,
    Gender STRING,
    Age INT,
    Nationality STRING,
    Airport_Name STRING,
    Airport_Country_Code STRING,
    Country_Name STRING,
    Airport_Continent STRING,
    Continents STRING,
    Departure_Date STRING,
    Arrival_Airport STRING,
    Pilot_Name STRING,
    Flight_Status STRING,
    Ticket_Type STRING,
    Passenger_Status STRING
);

CREATE TABLE IF NOT EXISTS audit_log (
    task STRING,
    affected_rows INT,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS passengers (
    Passenger_ID STRING,
    First_Name STRING,
    Last_Name STRING,
    Gender STRING,
    Age INT,
    Nationality STRING
);

CREATE TABLE IF NOT EXISTS airports (
    Airport_Name STRING,
    Airport_Country_Code STRING,
    Country_Name STRING,
    Airport_Continent STRING,
    Continents STRING
);

CREATE TABLE IF NOT EXISTS flights (
    Passenger_ID STRING,
    Departure_Date STRING,
    Arrival_Airport STRING,
    Flight_Status STRING,
    Ticket_Type STRING,
    Passenger_Status STRING,
    Pilot_Name STRING
);
