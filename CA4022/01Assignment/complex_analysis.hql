--Create and load tables
--Accidentally appended to table, producing wrong results, so included the drop line.

DROP TABLE IF EXISTS movies;
CREATE TABLE IF NOT EXISTS movies (movieId int, title string, year int, userId int, rating
float, ts int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' ;

-- Import clean data
LOAD DATA LOCAL INPATH '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/movies.csv' into table movies;

--Create and load tables
--Accidentally appended to table, producing wrong results, so included the drop line.
DROP TABLE IF EXISTS genres;
CREATE TABLE IF NOT EXISTS genres(movieId int, genre string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' ;

-- Import clean data
LOAD DATA LOCAL INPATH '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/genres.csv' into table genres;


/*Part 1
For this I will group by rating and get a count of each rating to find the rating distribution.
*/

INSERT OVERWRITE LOCAL DIRECTORY '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/rating_distribution_hive' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'

SELECT  rating, COUNT(rating)
FROM    movies
GROUP BY rating
ORDER BY rating

/*Part 2
For this part I will import the data from part 4. Then return the most popular rating.
*/

/*Create and load tables
Accidentally appended to the table, producing wrong results, so included the drop line.
*/

DROP TABLE IF EXISTS rating_distribution;
CREATE TABLE IF NOT EXISTS rating_distribution(rating float, rating_count int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' ;

-- Import clean data
LOAD DATA LOCAL INPATH '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/rating_distribution_hive' into table rating_distribution;

INSERT OVERWRITE LOCAL DIRECTORY '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/most_common_rating_hive' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'

SELECT  rating, rating_count
FROM    rating_distribution
ORDER BY rating_count DESC
LIMIT 1;


/*Part 3
For this part I will find the count of each rating for each genre.
I will need to use the two tables movies and genres.
*/

INSERT OVERWRITE LOCAL DIRECTORY '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/genre_rating_distribution_hive'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'

SELECT g.genre, m.rating, COUNT(m.rating) FROM genres g JOIN movies m
ON(g.movieId=m.movieId)
GROUP BY g.genre, m.rating
ORDER BY g.genre, m.rating;

