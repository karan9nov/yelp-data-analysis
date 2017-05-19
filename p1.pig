SET elephantbird.jsonloader.nestedLoad 'true';

--LOAD FROM THE JSON
loadJson = LOAD '/user/kx361/business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map []);

--GENERATE THE REQUIRED DATA
businessData = FOREACH loadJson GENERATE (int)json#'review_count' as review_count, json#'city' as city, json#'categories' as categories;

--FILTER THE DATA BY REQUIRED CITIES
us_business = FILTER businessData BY ( 
(city matches '.*Pittsburgh.*') OR (city matches '.*Charlotte.*') OR (city matches '.*Urbana-Champaign.*') OR (city matches '.*Phoenix.*') OR (city matches '.*Las Vegas.*') OR (city matches '.*Madison.*') OR (city matches '.*Cleveland.*'));

--FLATTEN THE DATA OUT
flattenedBusinessData = FOREACH us_business GENERATE review_count, city, FLATTEN(categories);

--GROUP THE DATA BY CITY AND CATEGORIES
groupedBusinessData = GROUP flattenedBusinessData BY (city,categories);

--PERFORM THE REQUIRED SUM OPERATION ON REVIEW_COUNT
finalData = FOREACH groupedBusinessData GENERATE group.city as city , group.categories as category, SUM(flattenedBusinessData.review_count);

--STORE THE DATA IN THE DIRECTORY
store finalData into './p1' using PigStorage(',') ;