SELECT
	titles.primary_title,votes
FROM
	titles JOIN (
	SELECT
		title_id,
		votes 
	FROM
		ratings 
	WHERE
		ratings.title_id IN (
		SELECT
			title_id 
		FROM
			crew 
		WHERE
			crew.person_id IN (
			SELECT
				person_id 
			FROM
				people 
			WHERE
				born = 1962 
				AND name LIKE '%Cruise%' 
			) 
		) 
	ORDER BY
		votes DESC 
	LIMIT 10 
	) as sub_query
	WHERE titles.title_id=sub_query.title_id;