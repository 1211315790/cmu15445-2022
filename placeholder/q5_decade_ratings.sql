SELECT
	(
		CAST ( premiered - premiered % 10 AS TEXT ) || 's' 
	) AS DECADE,
	round( AVG( rating ), 2 ) AS AVG_RATING,
	MAX( rating ) AS TOP_RATING,
	MIN( rating ) AS MIN_RATING,
	count( * ) AS NUM_RELEASES 
	
FROM
	ratings
	JOIN titles ON ratings.title_id = titles.title_id 
WHERE
	premiered IS NOT NULL 
GROUP BY
	premiered - premiered % 10 
ORDER BY
	round( AVG( rating ), 2 ) DESC,
	premiered - premiered % 10 ASC;