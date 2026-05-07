-- Total 6 basic

-- Khoa
-- cheking number of entity
SELECT
  (SELECT COUNT(*) FROM company) AS company_count,
  (SELECT COUNT(*) FROM country) AS country_count,
  (SELECT COUNT(*) FROM genre) AS genre_count,
  (SELECT COUNT(*) FROM keyword) AS keyword_count,
  (SELECT COUNT(*) FROM language) AS language_count,
  (SELECT COUNT(*) FROM movie) AS movie_count;
  
-- Random checking data
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
WHERE mv.movie_id in ('68718','293660','550','1726');

-- My
--//What are the latest movies and their original languages?//
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

-- //How many movies belong to each genre?//
SELECT 
    g.genre_name,
    COUNT(*) AS number_of_movies
FROM genre g
JOIN movie_genre mg
    ON g.genre_id = mg.genre_id
GROUP BY g.genre_name
ORDER BY number_of_movies DESC;

-- Bris

-- box office failures
SELECT *
FROM movie
WHERE revenue <= budget;

--//How hastotal movie revenue changed over time?//
WITH yearly_rev_overtime AS (
SELECT
YEAR(release_date) AS yr,
SUM(revenue) AS year_revenue
FROM movie
WHERE release_date <= ’2026-06-01’
GROUP BY YEAR(release_date)
)
SELECT *
FROM yearly_rev_overtime
ORDER BY yr;