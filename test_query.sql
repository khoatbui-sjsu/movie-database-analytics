SELECT COUNT(id) FROM movies;

SELECT 
    mv.movie_id,
    mv.title,
    (SELECT GROUP_CONCAT(cp.company_name ORDER BY cp.company_name)
     FROM movie_company mcp
     JOIN company cp ON mcp.company_id = cp.company_id
     WHERE mcp.movie_id = mv.movie_id
    ) AS companies,

    (SELECT GROUP_CONCAT(l.language_name ORDER BY l.language_name)
     FROM movie_language ml
     JOIN language l ON ml.language_code = l.language_code
     WHERE ml.movie_id = mv.movie_id
    ) AS languages_support

FROM movie mv
WHERE mv.movie_id = '68718';