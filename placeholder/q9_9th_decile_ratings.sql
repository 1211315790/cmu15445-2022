WITH tmp AS (
    SELECT name,
        ROUND(AVG_RATING, 2) AS AVG_RATING,
        NTILE(10) OVER (
            ORDER BY AVG_RATING ASC
        ) AS ranking
    FROM (
            SELECT name,
                AVG(rating) AS AVG_RATING
            FROM people
                JOIN crew ON people.person_id = crew.person_id
                JOIN titles ON crew.title_id = titles.title_id
                JOIN ratings ON titles.title_id = ratings.title_id
            WHERE born = 1955
                AND type = 'movie'
            GROUP BY people.person_id
        )
)
SELECT name,
    AVG_RATING
FROM tmp
WHERE ranking = 9
ORDER BY AVG_RATING DESC,
    name ASC;