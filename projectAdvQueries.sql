USE TMDBMovie;
----------------------
-- total 15 advance --
----------------------

-- Bris 5 queries

USE TMDBMovie;

-- 1. get the revenue running total of movie releases over the years up to 2026 (released)
WITH yearly_rev_overtime AS (
  SELECT YEAR(release_date) AS yr, SUM(revenue) AS year_revenue
  FROM movie
  WHERE release_date <= '2026-06-01'
  GROUP BY YEAR(release_date)
)
SELECT yr, year_revenue,
  SUM(year_revenue) -- running total prev to curr
  OVER (
    ORDER BY yr
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cum_revenue
FROM yearly_rev_overtime
ORDER BY yr;

-- 2. separate budgets into diff tiers
WITH tiers AS (
	select *, 
    ntile(5) over (order by budget) as bucket
    from movie
)
SELECT *,
	CASE WHEN bucket = 1 then 'Mega'
		when bucket = 2 then 'Mid'
        when bucket = 3 then 'Low'
        when bucket = 4 then 'Independent'
        when bucket = 5 then 'Micro'
	END budget_tier
from tiers;

-- 3. rank movies by revenue within each original language
WITH ranked_movies AS (
    SELECT movie_id, title, original_language_code, revenue,
	RANK() OVER ( PARTITION BY original_language_code ORDER BY revenue DESC) AS rev_rank
    FROM movie
)
SELECT *
FROM ranked_movies
WHERE rev_rank <= 3;

-- 4. do movies with more genres have higher revenue? since they appeal to wider audience
WITH movie_genre_count AS (
	SELECT movie_id, count(genre_id) as genre_count
	FROM movie_genre
    GROUP BY movie_id
)
SELECT mgc.genre_count, COUNT(*) AS movie_count, AVG(m.revenue - m.budget) AS avg_profit
FROM movie_genre_count mgc
JOIN movie m
ON mgc.movie_id = m.movie_id
WHERE m.revenue IS NOT NULL AND m.revenue > 0 AND m.budget IS NOT NULL AND m.budget > 0
GROUP BY mgc.genre_count
ORDER BY mgc.genre_count;


-- 5. which countries produce the highest-budget movies on average?
select * from (
select country_id, AVG(budget) as country_budget
from movie_country mc
JOIN movie m
ON mc.movie_id = m.movie_id
WHERE m.budget IS NOT NULL AND m.budget > 0
GROUP BY country_id
) as tb
join country c
on tb.country_id = c.country_id
order by country_budget desc;

-- 6. failures based on revenue
SELECT *
FROM movie
WHERE revenue > 0 AND revenue <= budget AND release_date <= '2026-06-01';


-- 7. indexing/performance
/*
Before:
'-> Sort: avg_vote DESC  (actual time=5058..5058 rows=19 loops=1)\n    
	-> Stream results  (cost=438616 rows=19) (actual time=747..5058 rows=19 loops=1)\n        
		-> Group aggregate: avg(m.vote_average)  (cost=438616 rows=19) (actual time=747..5057 rows=19 loops=1)\n            	
			-> Nested loop inner join  (cost=371776 rows=290087) (actual time=3.2..4964 rows=1.1e+6 loops=1)\n                
				-> Nested loop inner join  (cost=32725 rows=322319) (actual time=2.91..347 rows=1.19e+6 loops=1)\n                    
					-> Covering index scan on g using genre_name  (cost=2.9 rows=19) (actual time=1.23..1.27 rows=19 loops=1)\n                    
						-> Covering index lookup on mg using genre_id (genre_id = g.genre_id)  (cost=115 rows=16964) (actual time=0.525..16.6 rows=62774 loops=19)\n                
					-> Filter: ((year(m.release_date) <= 2026) and (m.vote_average is not null))  (cost=0.952 rows=0.9) (actual time=0.00377..0.0038 rows=0.92 loops=1.19e+6)\n                    
						-> Single-row index lookup on m using PRIMARY (movie_id = mg.movie_id)  (cost=0.952 rows=1) (actual time=0.00369..0.0037 rows=1 loops=1.19e+6)\n'
*/
-- vote avg based on movie genre
explain analyze 
select g.genre_name, AVG(vote_average) as avg_vote
from movie m
join movie_genre mg
on mg.movie_id = m.movie_id
join genre g
on g.genre_id = mg.genre_id
WHERE YEAR(release_date) <= 2026 AND vote_average IS NOT NULL
group by g.genre_name
order by avg_vote desc;

-- optimized from 4.5s to 1.6s
/*
After:
'-> Sort: avg_vote DESC  (actual time=1668..1668 rows=19 loops=1)\n    
	-> Table scan on <temporary>  (actual time=1668..1668 rows=19 loops=1)\n        
		-> Aggregate using temporary table  (actual time=1668..1668 rows=19 loops=1)\n            
			-> Nested loop inner join  (cost=1.05e+6 rows=847390) (actual time=0.213..1449 rows=1.1e+6 loops=1)\n                
				-> Nested loop inner join  (cost=753814 rows=847390) (actual time=0.197..1112 rows=1.1e+6 loops=1)\n                   
					-> Filter: ((movie.release_date <= DATE\'2026-12-31\') and (movie.vote_average is not null))  (cost=141158 rows=629077) (actual time=0.122..141 rows=1.09e+6 loops=1)\n                        
						-> Covering index range scan on movie using idx_movie_release_vote over (NULL < release_date <= \'2026-12-31\')  (cost=141158 rows=698974) (actual time=0.12..102 rows=1.09e+6 loops=1)\n                    
					-> Covering index lookup on mg using PRIMARY (movie_id = movie.movie_id)  (cost=0.839 rows=1.35) (actual time=742e-6..822e-6 rows=1.01 loops=1.09e+6)\n                
				-> Single-row index lookup on g using PRIMARY (genre_id = mg.genre_id)  (cost=0.25 rows=1) (actual time=223e-6..237e-6 rows=1 loops=1.1e+6)\n'

*/
CREATE INDEX idx_movie_release_date ON movie(release_date);
CREATE INDEX idx_movie_release_vote ON movie(release_date, vote_average, movie_id);

explain analyze WITH filtered_res AS (
  SELECT movie_id, vote_average
  FROM movie
  WHERE release_date <= '2026-06-01' AND vote_average IS NOT NULL
)
SELECT g.genre_name, AVG(fr.vote_average) AS avg_vote
FROM filtered_res fr
JOIN movie_genre mg ON mg.movie_id = fr.movie_id
JOIN genre g ON g.genre_id = mg.genre_id
GROUP BY g.genre_name
ORDER BY avg_vote DESC;

-- My
-- //Which movies outperform their language average?//
SELECT m.movie_id, m.title, l.language_name, m.revenue
FROM movie m
LEFT JOIN language l 
	ON m.original_language_code = l.language_code
JOIN (
SELECT original_language_code, AVG(revenue) as avg_lang_revenue
FROM movie
GROUP BY original_language_code) as lang_avg 
	ON m.original_language_code = lang_avg.original_language_code
WHERE m.revenue > lang_avg.avg_lang_revenue;

-- 2.Which genres have the longest movies?
SELECT g.genre_name, COUNT(*) AS movie_count,
AVG(m.runtime) AS avg_runtime
FROM movie m
JOIN movie_genre mg 
	ON m.movie_id = mg.movie_id
JOIN genre g 
	ON mg.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_runtime DESC;

-- 3. Which movie genres have the highest number of movies with the average audience ratings?
SELECT 
    g.genre_name,
    COUNT(DISTINCT m.movie_id) AS movie_count,
    ROUND(AVG(m.vote_average), 2) AS avg_rating
FROM movie m
JOIN movie_genre mg
    ON m.movie_id = mg.movie_id
JOIN genre g
    ON mg.genre_id = g.genre_id
WHERE m.vote_average IS NOT NULL
GROUP BY g.genre_name
ORDER BY movie_count DESC
LIMIT 15;

-- 4. What is the best-rated movie each year?
WITH ranked_year_movies AS (
    SELECT 
        YEAR(release_date) AS release_year,
        title,
        vote_average,
        vote_count,
        ROW_NUMBER() OVER (
            PARTITION BY YEAR(release_date)
            ORDER BY vote_average DESC, vote_count DESC
        ) AS rating_rank
    FROM movie
    WHERE release_date IS NOT NULL
      AND vote_count >= 500
)
SELECT 
    release_year,
    title,
    vote_average,
    vote_count
FROM ranked_year_movies
WHERE rating_rank = 1
ORDER BY release_year DESC;

-- 5.How are movies distributed across different genres in the dataset?
SELECT 
    g.genre_name,
    COUNT(DISTINCT m.movie_id) AS movie_count,
    ROUND(
        COUNT(DISTINCT m.movie_id) * 100.0 /
        SUM(COUNT(DISTINCT m.movie_id)) OVER (),
        2
    ) AS percentage
FROM movie m
JOIN movie_genre mg
    ON m.movie_id = mg.movie_id
JOIN genre g
    ON mg.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY movie_count DESC
LIMIT 10;
-- Khoa
-- 1.Show languages that don't appear in a movie
SELECT *
FROM language l
WHERE NOT EXISTS (
	SELECT 1
	FROM movie m
	WHERE l.language_code = m.original_language_code
    
)
LIMIT 5;


-- 2.Company has average movie rating higher than the overall average rating
WITH CompanyStats AS (
    SELECT 
        c.company_name,
        COUNT(mc.movie_id) as movie_count,
        AVG(m.vote_average) as avg_rating
    FROM company c
    JOIN movie_company mc ON c.company_id = mc.company_id
    JOIN movie m ON mc.movie_id = m.movie_id
    GROUP BY c.company_id, c.company_name
)
SELECT company_name, movie_count, avg_rating
FROM CompanyStats
WHERE movie_count > 5 
  AND avg_rating > (SELECT AVG(vote_average) FROM movie)
ORDER BY avg_rating DESC;


-- 3. Total 10 company has most total_revenue
SELECT 
    c.company_name, 
    rev.total_revenue
FROM company c
JOIN (
    SELECT 
        mc.company_id, 
        SUM(m.revenue) AS total_revenue
    FROM movie_company mc
    JOIN movie m ON mc.movie_id = m.movie_id
    WHERE m.revenue > 0
    GROUP BY mc.company_id
    ORDER BY total_revenue DESC
    LIMIT 10
) AS rev ON c.company_id = rev.company_id
ORDER BY rev.total_revenue DESC;

-- 4. Top 10 company has highest ROI ratio
WITH CompanyROI AS (
    SELECT 
        c.company_name,
        COUNT(m.movie_id) AS movie_count,
        SUM(m.budget) AS total_investment,
        SUM(m.revenue) AS total_return,
        (SUM(m.revenue) - SUM(m.budget)) / NULLIF(SUM(m.budget), 0) AS roi_multiplier
    FROM company c
    JOIN movie_company mc ON c.company_id = mc.company_id
    JOIN movie m ON mc.movie_id = m.movie_id
    WHERE m.budget > 0 AND m.revenue > 0
    GROUP BY c.company_id, c.company_name
)
SELECT * FROM CompanyROI 
WHERE movie_count >= 6
ORDER BY roi_multiplier DESC 
LIMIT 10;


-- 5.Movie Popularity "Z-Score"
SELECT 
    title, 
    popularity,
    ROUND((popularity - avg_pop) / stddev_pop, 2) AS popularity_z_score
FROM movie, 
(SELECT 
    AVG(popularity) AS avg_pop, 
    STDDEV(popularity) AS stddev_pop 
 FROM movie) AS stats
WHERE popularity IS NOT NULL
ORDER BY popularity_z_score DESC
LIMIT 15;


-- 6."Genre Specialists" by Country
SELECT 
    country_name, 
    genre_name, 
    movie_count
FROM (
    SELECT 
        cn.country_name,
        g.genre_name,
        COUNT(m.movie_id) AS movie_count,
        RANK() OVER (PARTITION BY cn.country_name ORDER BY COUNT(m.movie_id) DESC) as genre_rank
    FROM country cn
    JOIN movie_country mcn ON cn.country_id = mcn.country_id
    JOIN movie m ON mcn.movie_id = m.movie_id
    JOIN movie_genre mg ON m.movie_id = mg.movie_id
    JOIN genre g ON mg.genre_id = g.genre_id
    GROUP BY cn.country_id, cn.country_name, g.genre_id, g.genre_name
) AS RegionalGenres
WHERE genre_rank = 1
ORDER BY movie_count DESC;

-- 7. Percentage of Total Revenue by company
WITH IndustryTotal AS (
    SELECT SUM(revenue) as global_revenue FROM movie WHERE revenue > 0
),
CompanyTotals AS (
    SELECT 
        c.company_name,
        SUM(m.revenue) as company_revenue,
        COUNT(m.movie_id) as movie_count
    FROM company c
    JOIN movie_company mc ON c.company_id = mc.company_id
    JOIN movie m ON mc.movie_id = m.movie_id
    WHERE m.revenue > 0
    GROUP BY c.company_id, c.company_name
)
SELECT 
    company_name,
    company_revenue,
    movie_count,
    ROUND((company_revenue / (SELECT global_revenue FROM IndustryTotal)) * 100, 2) as market_share_percentage,
    DENSE_RANK() OVER (ORDER BY company_revenue DESC) as industry_rank
FROM CompanyTotals
ORDER BY company_revenue DESC
LIMIT 10;
