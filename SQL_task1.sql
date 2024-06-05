-- Create indexes for all queries
CREATE INDEX idx_rooms_id ON rooms(id);
CREATE INDEX idx_students_room_id ON students(room_id);
CREATE INDEX idx_students_birthday ON students(birthday);
CREATE INDEX idx_students_gender ON students(gender);



-- the Creating a list with all rooms with students and count of students for each room.
SELECT r.name as Room_name, COUNT(s.name) as Students_count
FROM rooms AS r 
LEFT JOIN students AS s ON r.id = s.room_id 
GROUP BY 1
ORDER BY 
  CAST(SUBSTRING(r.name FROM '#([0-9]+)') AS INTEGER);



-- the Creating a list of 5 room with smallest average age of students for each room.
WITH RoomAverage AS (
		SELECT
			r.id AS room_id,
			r.name AS room_name,
			ROUND(AVG(
					EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday)) + 
					EXTRACT(MONTH FROM AGE(CURRENT_DATE, s.birthday)) / 12 + 
					EXTRACT(DAY FROM AGE(CURRENT_DATE, s.birthday)) / 365
			),0
			) AS average_age
		FROM rooms AS r 
			JOIN students AS s ON r.id = s.room_id
		GROUP BY
			r.id,
			r.name
	)
SELECT
	room_name,
	average_age
FROM
	RoomAverage
ORDER BY
	average_age ASC
LIMIT
	5;



-- the Creating a list of 5 room with biggest age difference of students for each room.
WITH AgeRange AS (
    SELECT 
        r.id AS room_id,
        r.name AS room_name,
        MAX(extract(year from(S.BIRTHDAY)))  - MIN(extract(year from(S.BIRTHDAY))) AS age_range
    FROM 
        rooms AS r
    JOIN 
        students AS s ON r.id = s.room_id
    GROUP BY 
        r.id, r.name
)
SELECT 
    room_id, 
    room_name, 
    age_range
FROM 
    AgeRange
ORDER BY 
    age_range DESC
LIMIT 5;



--Explain how an index works in queries. For Example with first query.
EXPLAIN SELECT r.name as Room_name, COUNT(s.name) as Students_count
FROM rooms AS r 
LEFT JOIN students AS s ON r.id = s.room_id 
GROUP BY 1
ORDER BY 
  CAST(SUBSTRING(r.name FROM '#([0-9]+)') AS INTEGER);
