select name,2022-born AS age from people where 
died IS NULL and born>=1900 order by age desc,name limit 20;
