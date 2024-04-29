WITH tmp AS (
    SELECT DISTINCT (title)
    FROM akas
    WHERE title_id IN (
            SELECT title_id
            FROM titles
            WHERE primary_title LIKE '%House of the Dragon%'
                AND type = 'tvSeries'
        )
    ORDER BY title
)
SELECT GROUP_CONCAT(title, ', ')
FROM tmp;