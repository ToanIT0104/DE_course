USE beemovies;
# Q1: Số dòng của từng bảng
SELECT
	(SELECT COUNT(*) FROM movie) AS cnt_movies,
	(SELECT COUNT(*) FROM genre) AS cnt_genre,
	(SELECT COUNT(*) FROM names) AS cnt_names,
	(SELECT COUNT(*) FROM director_mapping) AS cnt_director,
	(SELECT COUNT(*) FROM ratings) AS cnt_ratings,
	(SELECT COUNT(*) FROM role_mapping) AS cnt_role;
    
# Q2: Cột nào trong bảng movies có giá trị null
SELECT
	SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS nulll_title,
	SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END) AS null_duration,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN date_published IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN worlwide_gross_income IS NULL THEN 1 ELSE 0 END) AS null_wgi,
    SUM(CASE WHEN languages IS NULL THEN 1 ELSE 0 END) AS null_languages,
    SUM(CASE WHEN production_company IS NULL THEN 1 ELSE 0 END) AS null_product_company	
FROM movie;

# Q3: Tổng số phim theo năm và theo tháng

# Theo năm
SELECT
	year(date_published),
    count(*) as total_movie
    from movie
    group by year(date_published)
    order by year(date_published);
    
# Theo tháng
SELECT
	month(date_published),
    count(*) as total_month
    from movie
    group by month(date_published)
    order by month(date_published);
    
# Q4: Số phim sản xuất ở Hoa Kỳ và Ấn Độ năm 2019
select
	year,
    country,
	count(*) as total
    from movie
    where (country like '%India%' or country like '%USA%') and year = 2019
    group by country;
    
# Q5: Các thể loại có trong tập dữ liệu
select
	genre
    from genre
    group by genre;
    
# Q6: Thể loại được sản xuất nhiều nhất
select
	genre,
    count(*) as cnt_genre
    from genre
    group by genre
    order by cnt_genre desc
    limit 1;
# Q7: Phim chỉ có 1 thể loại
select
    m.id,
    m.title,
    g.genre,
    count(g.genre) as cnt_genre
	from genre g
	join movie m on g.movie_id = m.id
	group by m.id
	having count(g.genre) < 2;
    
# Q8: thời lượng trung bình của mỗi thể loại
select
	g.genre as genre,
    avg(m.duration) as duration
	from movie m
    join genre g on m.id = g.movie_id
    group by g.genre;
    
# Q9: rank cuar thể loại phim kinh dị

select
	rankk,
    genre,
    cnt
from(
	select
	genre,
    count(genre) as cnt,
    row_number() over (order by count(genre) desc) as rankk
	from genre
    group by genre
) as ranked
where genre = 'Thriller';

# Q10: max min của các cột trong bảng ratings

select
	max(avg_rating) as max_average_rating,
    min(avg_rating) as min_average_rating,
    max(total_votes) as max_total_vote,
    min(total_votes) as min_total_vote,
    max(median_rating) as max_median_rating,
    min(median_rating) as min_median_rating
from ratings;

# Q11: top 10 phim có đánh giá tốt nhất
select
	m.title,
    avg(rt.avg_rating) as avg_ra,
    rank() over (order by avg(rt.avg_rating) desc) as rankk
	from movie m
    join ratings rt on m.id = rt.movie_id
    group by m.title
    order by avg(rt.avg_rating) desc
    limit 10;
    
# Q12: đếm số lượng phim theo median rating
select
	median_rating,
    count(median_rating) as movie_count
	from ratings
    group by median_rating
    order by count(median_rating) desc;
    
# Q13: công ty sản suất nhiều phim ăn khách nhất

select
	ranked.company,
    count_movie,
    rankk
from
(
	select
    m.production_company as company,
    COUNT(*) AS count_movie,
    rank() over(order by COUNT(*) desc) as rankk
from movie m
join ratings rt on rt.movie_id = m.id
where rt.avg_rating > 8 and m.production_company is not null
group by m.production_company
order by rankk
) as ranked
where ranked.rankk = 1;

# Q14: số bộ phim ở các thể loại có hơn  1000 lượt vote phát hành 3/2017 tại usa

select
	g.genre as genre,
    count(*) as count_movie
	from movie m
    join ratings rt on m.id = rt.movie_id
    join genre g on m.id = g.movie_id
    where rt.total_votes > 1000 
    and (YEAR(m.date_published) = 2017 AND MONTH(m.date_published) = 3)
    and m.country = 'USA'
    group by g.genre;
    
# Q15: Phim mỗi thẻ loại bắt đầu bằng The có avgrt > 8

select
	m.title,
    g.genre,
    rt.avg_rating
    from movie m
    join genre g on m.id = g.movie_id
    join ratings rt on m.id = rt.movie_id
    where m.title like 'The%' and rt.avg_rating > 8
    order by g.genre;
    
# Q16: 1/4/2018 - 1/4/2019 cos bnh bộ phim có avgrating = 8

select
	count(*) as count_movie
	from movie m
	join ratings rt on m.id = rt.movie_id
	where m.date_published BETWEEN '2018-04-01' AND '2019-04-01'
	and (rt.avg_rating = 8);
    
# Q17: Đức với ý nước nào có nhiều phiếu bầu hơn
    
select
	germany.total_vote_gemany,
    italy.total_vote_italy,
    if(germany.total_vote_gemany > italy.total_vote_italy, 'yes', 'no') as result
	from
    (
		(select
			sum(rt.total_votes) as total_vote_gemany
			from movie m
			join ratings rt on m.id = rt.movie_id
			where m.country = 'Germany'
			group by m.country
        ) as germany,
        (select
			sum(rt.total_votes) as total_vote_italy
			from movie m
			join ratings rt on m.id = rt.movie_id
			where m.country = 'Italy'
			group by m.country
        ) as italy
    );
    
# Q18: 
select
	sum(case when name is null then 1 else 0 end) as cnt_name,
    sum(case when height is null then 1 else 0 end) as cnt_height,
    sum(case when date_of_birth is null then 1 else 0 end) as cnt_birth,
    sum(case when known_for_movies is null then 1 else 0 end) as cnt_known
    from names;
# Q19:
with top_genres as (
    select g.genre
    from movie m
    join ratings rt on m.id = rt.movie_id
    join genre g on m.id = g.movie_id
    where rt.avg_rating > 8
    group by g.genre
    order by count(*) desc
    limit 3
),
movies_in_top_genres as (
    select m.id as movie_id
    from movie m
    join ratings rt on m.id = rt.movie_id
    join genre g on m.id = g.movie_id
    where rt.avg_rating > 8
      and g.genre in (select genre from top_genres)
),
directors_count as (
    select na.name as name_director,
           count(*) as cnt_movi
    from movies_in_top_genres mtg
    join director_mapping dm on mtg.movie_id = dm.movie_id
    join names na on na.id = dm.name_id
    group by na.name
)
select *
from directors_count
order by cnt_movi desc
limit 3;

# Q20:
with
top_movie as (
	select
    m.id as m_id
    from movie m
    join ratings rt on m.id = rt.movie_id
    where rt.median_rating >= 8
),
top_dv as (
	select
		na.name,
        count(*) as cnt_mo
    from movie m
    join role_mapping rm on m.id = rm.movie_id
    join names na on na.id = rm.name_id
    join ratings rt on m.id = rt.movie_id
    where rt.median_rating >= 8 and m.id in (select m_id from top_movie) and (rm.category = 'actor' or rm.category = 'actress')
    group by na.name
)
select * from top_dv
order by cnt_mo desc
limit 2;

# Q21:
select
	m.production_company as company,
    sum(rt.total_votes) as cnt_vote
	from movie m
    join ratings rt on m.id = rt.movie_id
    where m.production_company is not null
    group by m.production_company
    order by cnt_vote desc
    limit 3;
    
#Q22:
with india_movies as (
    select id
    from movie
    where country like '%India%'
),
actor_ratings as (
    select
        na.name as actor_name,
        count(*) as movie_count,
        sum(rt.total_votes) as total_votes,
        sum(rt.avg_rating * rt.total_votes) * 1.0 / sum(rt.total_votes) as actor_avg_rating
    from movie m
    join ratings rt on m.id = rt.movie_id
    join role_mapping rm on m.id = rm.movie_id
    join names na on na.id = rm.name_id
    where m.id in (select id from india_movies)
      and rm.category = 'actor'
    group by na.name
    having count(*) >= 5
),
ranked_actors as (
    select *,
        dense_rank() over (
            order by actor_avg_rating desc, total_votes desc
        ) as actor_rank
    from actor_ratings
)
select *
from ranked_actors
order by actor_rank;

#Q23
with hindi_india_movies as (
    select id
    from movie
    where country like '%India%' and languages like '%Hindi%'
),
actress_ratings as (
    select
        na.name as actress_name,
        count(*) as movie_count,
        sum(rt.total_votes) as total_votes,
        sum(rt.avg_rating * rt.total_votes) * 1.0 / sum(rt.total_votes) as actress_avg_rating
    from movie m
    join ratings rt on m.id = rt.movie_id
    join role_mapping rm on m.id = rm.movie_id
    join names na on na.id = rm.name_id
    where m.id in (select id from hindi_india_movies)
      and rm.category = 'actress'
    group by na.name
    having count(*) >= 3
),
ranked_actresses as (
    select *,
        dense_rank() over (
            order by actress_avg_rating desc, total_votes desc
        ) as actress_rank
    from actress_ratings
)
select *
from ranked_actresses
order by actress_rank
limit 5;

# Q24:
select
    m.id as movie_id,
    m.title as movie_title,
    rt.avg_rating,
    case
        when rt.avg_rating > 8 then 'Superhit movie'
        when rt.avg_rating between 7 and 8 then 'Hit movie'
        when rt.avg_rating between 5 and 7 then 'One-time-watch movie'
        else 'Flop movie'
    end as rating_category
from movie m
join ratings rt on m.id = rt.movie_id
join genre g on m.id = g.movie_id
where g.genre = 'Thriller';

# Q25:
with genre_duration as (
    select
        g.genre,
        avg(m.duration) as avg_duration
    from movie m
    join genre g on m.id = g.movie_id
    where m.duration is not null
    group by g.genre
),
genre_stats as (
    select
        genre,
        avg_duration,
        sum(avg_duration) over (order by genre) as running_total_duration,
        avg(avg_duration) over (order by genre rows between 2 preceding and current row) as moving_avg_duration
    from genre_duration
)
select *
from genre_stats;

# Q26:
with top_genres as (
    select g.genre
    from genre g
    group by g.genre
    order by count(g.movie_id) desc
    limit 3
),

filtered_movies as (
    select
        g.genre,
        m.title as movie_name,
        m.year,
        m.worlwide_gross_income
    from movie m
    join genre g on m.id = g.movie_id
    where g.genre in (select genre from top_genres)
        and m.worlwide_gross_income is not null
        and m.year is not null
),

ranked_movies as (
    select
        genre,
        year,
        movie_name,
        cast(replace(worlwide_gross_income, '$', '') as decimal) as income,
        rank() over (
            partition by genre, year
            order by cast(replace(worlwide_gross_income, '$', '') as decimal) desc
        ) as movie_rank
    from filtered_movies
)

select
    genre,
    year,
    movie_name,
    concat('$', income) as worldwide_gross_income,
    movie_rank
from ranked_movies
where movie_rank <= 5
order by genre, year, movie_rank;

#Q27:

with multilingual_hits as (
    select
        m.production_company,
        m.id as movie_id
    from movie m
    join ratings r on m.id = r.movie_id
    where r.median_rating >= 8
        and m.languages like '%,%' -- Đa ngôn ngữ: có dấu phẩy trong danh sách ngôn ngữ
        and m.production_company is not null
),
production_hit_count as (
    select
        production_company,
        count(movie_id) as movie_count
    from multilingual_hits
    group by production_company
),
ranked_production as (
    select
        production_company,
        movie_count,
        rank() over (order by movie_count desc) as prod_comp_rank
    from production_hit_count
)
select *
from ranked_production
where prod_comp_rank <= 2;

#Q28:
with drama_superhit_movies as (
    select
        m.id as movie_id,
        m.title,
        rt.avg_rating,
        rt.total_votes
    from movie m
    join ratings rt on m.id = rt.movie_id
    join genre g on m.id = g.movie_id
    where g.genre = 'drama'
      and rt.avg_rating > 8
),

actress_movies as (
    select
        na.name as actress_name,
        dsm.movie_id,
        dsm.avg_rating,
        dsm.total_votes
    from drama_superhit_movies dsm
    join role_mapping rm on dsm.movie_id = rm.movie_id
    join names na on rm.name_id = na.id
    where rm.role = 'actress'
),

actress_stats as (
    select
        actress_name,
        sum(total_votes) as total_votes,
        count(*) as movie_count,
        sum(avg_rating * total_votes) / sum(total_votes) as actress_avg_rating
    from actress_movies
    group by actress_name
)

select
    actress_name,
    total_votes,
    movie_count,
    round(actress_avg_rating, 2) as actress_avg_rating,
    rank() over (order by movie_count desc, total_votes desc) as actress_rank
from actress_stats
order by actress_rank
limit 3;


#Q29






    



    

    

