GET_FILM_WORKS = """
SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'person_role', pfw.role,
               'person_id', p.id,
               'person_name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
   ) as persons,
   array_agg(DISTINCT g.name) as genres
FROM film_work fw
LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN person p ON p.id = pfw.person_id
LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN genre g ON g.id = gfw.genre_id
WHERE fw.id IN %(ids)s
GROUP BY fw.id
ORDER BY fw.modified;
"""

GET_PERSONS = """
SELECT id, modified
FROM person
WHERE modified > %s
ORDER BY modified;
"""

GET_GENRES = """
SELECT id, modified
FROM genre
WHERE modified > %s
ORDER BY modified;
"""

GET_MOVIES = """
SELECT id, modified
FROM film_work
WHERE modified > %s
ORDER BY modified;
"""

GET_FILM_WORKS_IDS = """
SELECT fw.id, fw.modified
FROM film_work fw
LEFT JOIN %(table)s tfw ON tfw.film_work_id = fw.id
WHERE tfw.%(colum)s IN %(ids)s
ORDER BY fw.modified;
"""
