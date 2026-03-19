SELECT
  (SELECT COUNT(*) FROM company) AS company_count,
  (SELECT COUNT(*) FROM country) AS country_count,
  (SELECT COUNT(*) FROM genre) AS genre_count,
  (SELECT COUNT(*) FROM keyword) AS keyword_count,
  (SELECT COUNT(*) FROM language) AS language_count,
  (SELECT COUNT(*) FROM movie) AS movie_count;
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

//2 basic SQL queries//
//What are the latest movies and their original languages?//
SELECT 
    m.movie_id,
    m.title,
    l.language_name AS original_language,
    m.release_date
FROM movie m
LEFT JOIN language l
    ON m.original_language_code = l.language_code
WHERE m.release_date IS NOT NULL
ORDER BY m.release_date DESC
LIMIT 10;

//How many movies belong to each genre?//
SELECT 
    g.genre_name,
    COUNT(*) AS number_of_movies
FROM genre g
JOIN movie_genre mg
    ON g.genre_id = mg.genre_id
GROUP BY g.genre_name
ORDER BY number_of_movies DESC;

//2 advanced SQL queries//
//What are the top 3 highest-revenue movies in each original language?//
WITH ranked_movies AS (
    SELECT
        m.movie_id,
        m.title,
        l.language_name AS original_language,
        m.revenue,
        RANK() OVER (
            PARTITION BY m.original_language_code
            ORDER BY m.revenue DESC
        ) AS revenue_rank
    FROM movie m
    LEFT JOIN language l
        ON m.original_language_code = l.language_code
    WHERE m.revenue IS NOT NULL
)
SELECT
    movie_id,
    title,
    original_language,
    revenue,
    revenue_rank
FROM ranked_movies
WHERE revenue_rank <= 3
ORDER BY original_language, revenue_rank, revenue DESC;

//Which movies earned more than the average revenue of movies in the same original language?//
SELECT
    m.movie_id,
    m.title,
    l.language_name AS original_language,
    m.revenue
FROM movie m
LEFT JOIN language l
    ON m.original_language_code = l.language_code
WHERE m.revenue IS NOT NULL
  AND m.revenue > (
      SELECT AVG(m2.revenue)
      FROM movie m2
      WHERE m2.original_language_code = m.original_language_code
        AND m2.revenue IS NOT NULL
  )
ORDER BY l.language_name, m.revenue DESC;