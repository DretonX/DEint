-- INDEXES
CREATE INDEX idx_actor_first_name ON public.actor (first_name);
CREATE INDEX idx_actor_last_name ON public.actor (last_name);
CREATE INDEX idx_film_title ON public.film (title);
CREATE INDEX idx_film_rating ON public.film (rating);
CREATE INDEX idx_customer_email ON public.customer (email);
CREATE INDEX idx_inventory_film_id ON public.inventory (film_id);
CREATE INDEX idx_inventory_store_id ON public.inventory (store_id);
CREATE INDEX idx_rental_rental_date ON public.rental (rental_date);
CREATE INDEX idx_rental_return_date ON public.rental (return_date);
CREATE INDEX idx_payment_customer_id ON public.payment (customer_id);
CREATE INDEX idx_payment_amount ON public.payment (amount);




-- 1
-- Displayed count of films for each category and sort result by DESC.

SELECT
	C.NAME,
	COUNT(FILM_ID)
FROM
	CATEGORY AS C
	LEFT JOIN FILM_CATEGORY AS FC ON C.CATEGORY_ID = FC.CATEGORY_ID
GROUP BY
	C.NAME
ORDER BY
	2 DESC


-- 2
-- Displayed actors with biggest count of films with their participation that were rented (top 10)

SELECT
	CONCAT(A.FIRST_NAME,' ', A.LAST_NAME) AS "ACTOR NAME",
	COUNT(R.INVENTORY_ID) AS "COUNT OF RENT"
FROM
	ACTOR AS A
	JOIN FILM_ACTOR AS FA ON A.ACTOR_ID = FA.ACTOR_ID
	JOIN INVENTORY AS I ON FA.FILM_ID = I.FILM_ID
	JOIN RENTAL AS R ON I.INVENTORY_ID = R.INVENTORY_ID
GROUP BY
	A.ACTOR_ID
ORDER BY
	2 DESC
LIMIT
	10


-- 3
-- Display One category with highest rent hours

SELECT 
C.NAME AS CATEGORY_NAME, SUM(P.AMOUNT) AS TOTAL_AMOUNT 
	FROM PAYMENT AS P 
	JOIN RENTAL AS R ON P.RENTAL_ID = R.RENTAL_ID
	JOIN INVENTORY AS I ON R.INVENTORY_ID = I.INVENTORY_ID
	JOIN FILM AS F ON I.FILM_ID = F.FILM_ID
	JOIN FILM_CATEGORY AS FC ON F.FILM_ID = FC.FILM_ID
	JOIN CATEGORY AS C ON FC.CATEGORY_ID = C.CATEGORY_ID 
GROUP BY C.NAME
ORDER BY TOTAL_AMOUNT DESC 
	LIMIT 1;


-- 4 
-- Displayed films whose is not in inventory table.

SELECT 
    F.TITLE
FROM 
    FILM AS F
LEFT JOIN 
    INVENTORY AS I ON F.FILM_ID = I.FILM_ID
WHERE 
    I.INVENTORY_ID IS NULL;


-- 5
-- Dispayed top3 short list but but if any Actors has a the same rank than they will be in short list to, after ranking in window function.

WITH ACTOR_RANK AS (
    SELECT
        CONCAT(A.FIRST_NAME,' ',A.LAST_NAME) AS ACTOR_NAME,
        COUNT(DISTINCT FA.FILM_ID) AS FILM_COUNT,
        DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT FA.FILM_ID) DESC) AS RANK
    FROM ACTOR AS A
    JOIN FILM_ACTOR AS FA ON A.ACTOR_ID = FA.ACTOR_ID
    JOIN FILM_CATEGORY AS FC ON FA.FILM_ID = FC.FILM_ID
    JOIN CATEGORY AS C ON FC.CATEGORY_ID = C.CATEGORY_ID
    WHERE C.NAME = 'Children'
    GROUP BY A.ACTOR_ID, A.FIRST_NAME, A.LAST_NAME
)
SELECT
    ACTOR_NAME,
    FILM_COUNT
FROM ACTOR_RANK
WHERE RANK <= 3;


-- 6
-- Displayed count of active and inactive customers for each city

SELECT
	CI.CITY,
	COUNT(*) AS TOTAL_CUSTOMERS, -- This statement needed for check of correct count 
	SUM(CASE WHEN CU.ACTIVE = 1 THEN 1 ELSE 0 END) AS ACTIVE_CUSTOMERS,
  	SUM(CASE WHEN CU.ACTIVE = 0 THEN 1 ELSE 0 END) AS INACTIVE_CUSTOMERS
	FROM CITY AS CI
	JOIN ADDRESS AS AD ON CI.CITY_ID = AD.CITY_ID
	JOIN CUSTOMER AS CU ON AD.ADDRESS_ID = CU.ADDRESS_ID
	GROUP BY CI.CITY 
	ORDER BY INACTIVE_CUSTOMERS DESC, ACTIVE_CUSTOMERS DESC;


-- 7
-- Displayed category with highest count of hours rental film for the two types of city: 
-- 1 - It's city starting with "a", 2 - It's city with "-"

WITH TIME_DATA AS (
    SELECT
        C.CITY,
        CAT.NAME AS CATEGORY_NAME,
        COALESCE(ROUND(SUM(EXTRACT(EPOCH FROM (R.RETURN_DATE - R.RENTAL_DATE)) / 3600),0), 0) AS TOTAL_HOURS
    FROM RENTAL AS R
    JOIN INVENTORY AS I ON R.INVENTORY_ID = I.INVENTORY_ID
    JOIN FILM AS F ON I.FILM_ID = F.FILM_ID
    JOIN FILM_CATEGORY AS FC ON F.FILM_ID = FC.FILM_ID
    JOIN CATEGORY AS CAT ON FC.CATEGORY_ID = CAT.CATEGORY_ID
    JOIN CUSTOMER AS CU ON R.CUSTOMER_ID = CU.CUSTOMER_ID
    JOIN ADDRESS AS A ON CU.ADDRESS_ID = A.ADDRESS_ID
    JOIN CITY AS C ON A.CITY_ID = C.CITY_ID
    WHERE (C.CITY LIKE 'a%' OR C.CITY LIKE '%-%') AND R.RETURN_DATE IS NOT NULL
    GROUP BY C.CITY, CAT.NAME
),
CATEGORY_RANK AS (
    SELECT
        CATEGORY_NAME,
        TOTAL_HOURS,
        CASE
            WHEN CITY LIKE 'a%' THEN 'Cities starting with "a"'
            WHEN CITY LIKE '%-%' THEN 'Cities with "-"'
        END AS CITY_GROUP,
        RANK() OVER (PARTITION BY CASE
        								WHEN CITY LIKE 'a%' THEN 'Cities starting with "a"'
										WHEN CITY LIKE '%-%' THEN 'Cities with "-"'
                                    	END ORDER BY TOTAL_HOURS DESC) AS RANK
	FROM TIME_DATA
)
SELECT
    CITY_GROUP,
    CATEGORY_NAME,
    TOTAL_HOURS
FROM CATEGORY_RANK
WHERE rank = 1;









