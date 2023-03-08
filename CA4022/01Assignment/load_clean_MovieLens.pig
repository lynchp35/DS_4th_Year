DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

-- load movies.csv using CSVExcelStorage(), (avoid the comma in the table value problem)

movies = LOAD 'ml-latest-small/movies.csv' using CSVExcelStorage() AS (movieId:int,
title:chararray, genres:chararray);

ratings = LOAD  'ml-latest-small/ratings.csv'  using CSVExcelStorage() AS (userId:int, movieId:int, rating:float, timestamp:int);

-- Remove duplicates

movies = DISTINCT movies;
ratings = DISTINCT ratings;
 
/*
Loop through the movies dataset, generate the movieId,
split the genres string using the TOKENIZE function on '|',
then use the FLATTEN function to convert each element of
the genre bag to a unique row.
*/


movieId_with_genres = FOREACH movies GENERATE movieId AS movieId, FLATTEN(TOKENIZE(genres,'|')) as genres;


/*
Again loop through each row, get the title name and year
by getting two substrings. The title name is found by
getting a substring from the first element of the
string up to 7 before the last element. Then for the year
I start 5 before the end up to 1 before the end.
*/

split_title_year = FOREACH movies GENERATE movieId AS movieId, SUBSTRING(title, 0, (int)SIZE(title) - 7) AS title,
SUBSTRING(title, (int)SIZE(title) - 5, (int)SIZE(title)-1) AS year:int;

movies_with_ratings = JOIN split_title_year by movieId, ratings by movieId;

movies_with_ratings = FOREACH movies_with_ratings GENERATE $0 as movieId,
$1 as title,
$2 as year,
$3 as userId,
$5 as rating,
$6 as timestamp;

-- save tables , change from csv to tsv (avoid the comma in the table value problem)

STORE movies_with_ratings INTO 'ml-latest-small/processed_data/movies.csv'  USING PigStorage('\t');
STORE movieId_with_genres INTO 'ml-latest-small/processed_data/genres.csv'  USING PigStorage('\t');

