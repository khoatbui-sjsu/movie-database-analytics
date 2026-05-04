USE TMDBMovie;
----------------------
-- total 15 advance --
----------------------

-- Bris 5 queries

-- 1.get the revenue running total of movie releases over the years
Select release_date, revenue, original_title,
SUM(revenue) OVER(PARTITION BY YEAR(release_date) order by release_date) AS yearly_running_total
from movie
WHERE revenue > 10000;

-- buget vs popularity

-- 2.vote avg based on movie genre
select g.genre_name, AVG(vote_average) as avg_vote
from movie m
join movie_genre mg
on mg.movie_id = m.movie_id
join genre g
on g.genre_id = mg.genre_id
WHERE vote_average <> 0
group by g.genre_name
limit 5;

-- vote count vs revenue

-- 3.separate budgets into diff tiers
WITH tiers AS (
	select *, 
    ntile(5) over (order by budget) as bucket
    from movie
)
SELECT *,
	CASE WHEN bucket = 1 then 'Mega'
		when bucket = 2 then 'Mid'
        when bucket = 3 then 'Cheap'
        when bucket = 4 then 'Independent'
        when bucket = 5 then 'Micro'
	END budget_tier
from tiers;

-- 4.Rank movies by revenue within each original language
WITH ranked_movies AS (
    SELECT movie_id, title, original_language_code, revenue,
	RANK() OVER ( PARTITION BY original_language_code ORDER BY revenue DESC) AS rev_rank
    FROM movie
)
SELECT *
FROM ranked_movies
WHERE rev_rank <= 3;

-- 5.do movies with more genres have higher revenue? since they appeal to wider audience
WITH movie_genre_count AS (
	SELECT movie_id, count(genre_id) as genre_count
	FROM movie_genre
    GROUP BY movie_id
)
SELECT mgc.genre_count, COUNT(*) AS movie_count, AVG(m.revenue - m.budget) AS avg_profit
FROM movie_genre_count mgc
JOIN movie m
ON mgc.movie_id = m.movie_id
WHERE m.revenue IS NOT NULL
  AND m.revenue > 0
  AND m.budget IS NOT NULL
  AND m.budget > 0
GROUP BY mgc.genre_count
ORDER BY mgc.genre_count;

-- 6.Which countries produce the highest-budget movies on average?
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

-- My

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