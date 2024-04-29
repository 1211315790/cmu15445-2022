SELECT name
FROM people
WHERE person_id IN (
        SELECT person_id
        FROM crew
        WHERE title_id IN (
                SELECT title_id
                FROM crew
                WHERE title_id IN (
                        SELECT title_id
                        FROM crew
                        WHERE person_id IN (
                                SELECT person_id
                                FROM people
                                WHERE name = 'Nicole Kidman'
                            )
                    )
            )
            AND (
                category = 'actor'
                OR category = 'actress'
            )
    )
order by name;
;