SELECT
	count( * ) 
FROM
	titles 
WHERE
	premiered IN (
	select premiered from titles where primary_title='Army of Thieves');