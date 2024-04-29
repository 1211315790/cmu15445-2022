SELECT name,num FROM ( SELECT person_id,COUNT(person_id)as num FROM crew GROUP BY  person_id ORDER BY COUNT(person_id) DESC limit 20) as q join people  WHERE q.person_id=people.person_id ;
