MERGE INTO passengers AS target
USING (
    SELECT DISTINCT Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality
    FROM airline_raw_data
) AS source
ON target.Passenger_ID = source.Passenger_ID
WHEN NOT MATCHED THEN
    INSERT (Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality)
    VALUES (source.Passenger_ID, source.First_Name, source.Last_Name, source.Gender, source.Age, source.Nationality);

MERGE INTO airports AS target
USING (
    SELECT DISTINCT Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents
    FROM airline_raw_data
) AS source
ON target.Airport_Name = source.Airport_Name AND target.Airport_Country_Code = source.Airport_Country_Code
WHEN NOT MATCHED THEN
    INSERT (Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents)
    VALUES (source.Airport_Name, source.Airport_Country_Code, source.Country_Name, source.Airport_Continent, source.Continents);

MERGE INTO flights AS target
USING (
    SELECT Passenger_ID, Departure_Date, Arrival_Airport, Flight_Status, Ticket_Type, Passenger_Status, Pilot_Name
    FROM airline_raw_data
) AS source
ON target.Passenger_ID = source.Passenger_ID AND target.Departure_Date = source.Departure_Date AND target.Arrival_Airport = source.Arrival_Airport
WHEN NOT MATCHED THEN
    INSERT (Passenger_ID, Departure_Date, Arrival_Airport, Flight_Status, Ticket_Type, Passenger_Status, Pilot_Name)
    VALUES (source.Passenger_ID, source.Departure_Date, source.Arrival_Airport, source.Flight_Status, source.Ticket_Type, source.Passenger_Status, source.Pilot_Name);

CREATE OR REPLACE TABLE nationality_monthly_flights AS
WITH monthly_data AS (
    SELECT Nationality, TO_CHAR(TO_DATE(Departure_Date, 'MM/DD/YYYY'), 'YYYY-MM') AS month_year, COUNT(*) AS flight_count
    FROM airline_raw_data
    GROUP BY Nationality, month_year
)
SELECT *
FROM monthly_data
PIVOT (
    MAX(flight_count) FOR month_year IN (SELECT DISTINCT month_year FROM monthly_data)
);
