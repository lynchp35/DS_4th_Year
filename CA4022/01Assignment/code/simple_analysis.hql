--Create and load tables
--Accidentally appended to table, producing wrong results, so included the drop line.
DROP TABLE IF EXISTS movies;
CREATE TABLE IF NOT EXISTS movies (movieId int, title string, year int, userId int, rating
float, ts int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' ;

-- Import clean data
LOAD DATA LOCAL INPATH '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/movies.csv' into table movies;

/* Part 1
Same idea as part 1 for simple_analysis.pig.
Results are output to hive_01.csv
*/


INSERT OVERWRITE LOCAL DIRECTORY '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/hive_01.csv' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT title, COUNT(*) as watch_count FROM movies GROUP BY title
ORDER BY watch_count DESC
LIMIT 10;

/*Part 2
Similar to part 2 for pig but since I already have the data on each rating [4.0,5.0].
I will only return the count of ratings above 4.0. 
*/

INSERT OVERWRITE LOCAL DIRECTORY '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/hive_02.csv' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT title, COUNT(*) as rating_count 
FROM movies WHERE rating >= 4.0
GROUP BY title
ORDER BY rating_count DESC
LIMIT 10;

/*Part 3
Similar to part 3 for pig but since I already have the data on users with the highest average rating and the most movies rated. 
I will only return users with the highest average rating.
*/

INSERT OVERWRITE LOCAL DIRECTORY '/home/lynchp35/CA4022_01_Assignment/ml-latest-small/processed_data/hive_03.csv' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT  userId, AVG(rating) AS average_rating
From movies GROUP BY userId
ORDER BY average_rating DESC
LIMIT 10;

