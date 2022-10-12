-- Import both genres and movies table

genres = LOAD 'ml-latest-small/processed_data/genres.csv' USING PigStorage('\t') AS (movieId:int,
genres:chararray);

movies = LOAD 'ml-latest-small/processed_data/movies.csv' USING PigStorage('\t') AS (movieId:int,
title:chararray,
year:int,
userId:int,
rating:float,
timestamp);

/* 
Part 1
Using movies.csv, I want to return a count of how many times a title is meantioned.
Return the top 10 

*/


grouped_movies = GROUP movies BY title;
movie_count = FOREACH grouped_movies GENERATE group as title,COUNT($1) as count:int ;
movie_count = ORDER movie_count BY count DESC;
top_ten_rated_movies = LIMIT movie_count 10;

-- DUMP top_ten_rated_movies;

/*
Part 2
I want to find the top ten movies with the most 4 star or above ratings.
Find the count of 4, 4.5 and 5.
I will use this to show the distribution of ratings.

*/

movies_with_high_ratings = FILTER movies by (rating >= 4);

grouped_movies_with_high_ratings  = GROUP movies_with_high_ratings BY title;
movies_with_high_ratings_count = FOREACH grouped_movies_with_high_ratings GENERATE group as title,
COUNT($1) as count:int;
movies_with_high_ratings_count = ORDER movies_with_high_ratings_count BY count DESC;
top_ten_high_rated_movies = LIMIT movies_with_high_ratings_count 10;

-- Output is title, count(ratings >=4), sorted by count.
-- DUMP top_ten_high_rated_movies;

-- More work to find the distribution of ratings for top ten films.

movies_rated_4 = FILTER movies by (rating == 4);
movies_rated_4_half = FILTER movies by (rating == 4.5);
movies_rated_5 = FILTER movies by (rating == 5);

grouped_movies_rated_4 = GROUP movies_rated_4 BY title;
grouped_movies_rated_4_half  = GROUP movies_rated_4_half BY title;
grouped_movies_rated_5  = GROUP movies_rated_5 BY title;

movies_with_4_ratings_count = FOREACH grouped_movies_rated_4 
GENERATE group as title,COUNT($1) as count:int ;

movies_with_4_half_ratings_count = FOREACH grouped_movies_rated_4_half 
GENERATE group as title,COUNT($1) as count:int ;

movies_with_5_ratings_count = FOREACH grouped_movies_rated_5 
GENERATE group as title,COUNT($1) as count:int ;

tmp_movies_join = JOIN top_ten_high_rated_movies BY title LEFT OUTER, movies_with_5_ratings_count BY title;

tmp_movies_join = FOREACH tmp_movies_join GENERATE $0 as title,
$1 as count:int,
$3 as rating_5f:int;

tmp_movies_join = JOIN tmp_movies_join BY title LEFT OUTER, movies_with_4_half_ratings_count BY title;

tmp_movies_join = FOREACH tmp_movies_join GENERATE $0 as title,
$1 as count:int,
$2 as rating_5f:int,
$4 as rating_4_half:int;

tmp_movies_join = JOIN tmp_movies_join BY title LEFT OUTER, movies_with_4_ratings_count BY title;

top_high_rated_movies_breakdown  = FOREACH tmp_movies_join GENERATE $0 as title,
$1 as count:int,
$2 as rating_5f:int,
$3 as rating_4_half:int,
$5 as rating_4:int;

-- Output in format title, count(ratings >=4), count(ratings==5), count(ratings==4.5), count(ratings==4)
-- Sorted by count(ratings >= 4)
-- DUMP top_high_rated_movies_breakdown;


/*
Part 3
I want to return a the mean rating of a user.
Return top 10

*/

-- top_ten_movie_fans

grouped_user = GROUP movies BY userId;
user_average_rating = FOREACH grouped_user GENERATE group as userId, 
AVG(movies.rating) as average_rating, COUNT(movies.rating) as number_rated;

top_users = ORDER user_average_rating BY average_rating DESC;
top_ten_users = LIMIT top_users 10;

-- DUMP top_ten_users;

biggest_rater = ORDER user_average_rating BY number_rated DESC;
top_ten_biggest_rater = LIMIT biggest_rater 10;

-- DUMP top_ten_biggest_rater;

all_user_ratings = UNION top_ten_users, top_ten_biggest_rater;

-- DUMP all_user_ratings;
STORE all_user_ratings INTO 'ml-latest-small/processed_data/user_data.csv'  USING PigStorage('\t');
STORE top_ten_rated_movies  INTO 'ml-latest-small/processed_data/popular_movies.csv'  USING PigStorage('\t');
STORE top_high_rated_movies_breakdown INTO 'ml-latest-small/processed_data/highly_rated_movies.csv'  USING PigStorage('\t');
