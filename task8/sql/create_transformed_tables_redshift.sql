-- For passengers table
DELETE FROM passengers
USING (
    SELECT DISTINCT Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality
    FROM airline_raw_data
) AS source
WHERE passengers.Passenger_ID = source.Passenger_ID;

INSERT INTO passengers (Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality)
SELECT DISTINCT Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality
FROM airline_raw_data;

-- For airports table
DELETE FROM airports
USING (
    SELECT DISTINCT Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents
    FROM airline_raw_data
) AS source
WHERE airports.Airport_Name = source.Airport_Name AND airports.Airport_Country_Code = source.Airport_Country_Code;

INSERT INTO airports (Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents)
SELECT DISTINCT Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents
FROM airline_raw_data;

-- For flights table
DELETE FROM flights
USING (
    SELECT Passenger_ID, Departure_Date, Arrival_Airport, Flight_Status, Ticket_Type, Passenger_Status, Pilot_Name
    FROM airline_raw_data
) AS source
WHERE flights.Passenger_ID = source.Passenger_ID AND flights.Departure_Date = source.Departure_Date AND flights.Arrival_Airport = source.Arrival_Airport;

INSERT INTO flights (Passenger_ID, Departure_Date, Arrival_Airport, Flight_Status, Ticket_Type, Passenger_Status, Pilot_Name)
SELECT Passenger_ID, Departure_Date, Arrival_Airport, Flight_Status, Ticket_Type, Passenger_Status, Pilot_Name
FROM airline_raw_data;

-- Create nationality_monthly_flights table
DROP TABLE IF EXISTS nationality_monthly_flights;

CREATE TABLE nationality_monthly_flights AS
WITH monthly_data AS (
    SELECT
        Nationality,
        TO_CHAR(TO_DATE(Departure_Date, 'MM/DD/YYYY'), 'YYYY-MM') AS month_year,
        COUNT(*) AS flight_count
    FROM airline_raw_data
    GROUP BY Nationality, month_year
),
distinct_months AS (
    SELECT DISTINCT month_year
    FROM monthly_data
    ORDER BY month_year
)
SELECT
    md.Nationality,
    MAX(CASE WHEN md.month_year = dm.month_year THEN md.flight_count ELSE 0 END) AS flight_count,
    dm.month_year
FROM
    monthly_data md
CROSS JOIN
    distinct_months dm
GROUP BY
    md.Nationality, dm.month_year
ORDER BY
    md.Nationality, dm.month_year;