USE TMDBMovie;

-- get the revenue running total of movie releases over the years
Select release_date, revenue, original_title,
SUM(revenue) OVER(PARTITION BY YEAR(release_date) order by release_date) AS yearly_running_total
from movie;

-- buget vs popularity

-- vote avg based on movie genre
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

-- separate budgets into diff tiers
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

-- Rank movies by revenue within each original language
WITH ranked_movies AS (
    SELECT movie_id, title, original_language_code, revenue,
	RANK() OVER ( PARTITION BY original_language_code ORDER BY revenue DESC) AS rev_rank
    FROM movie
)
SELECT *
FROM ranked_movies
WHERE rev_rank <= 3;

-- box office failures
SELECT *
FROM movie
WHERE revenue <= budget;

-- do movies with more genres have higher revenue? since they appeal to wider audience
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


-- Which countries produce the highest-budget movies on average?
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

