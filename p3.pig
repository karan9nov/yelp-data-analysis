SET elephantbird.jsonloader.nestedLoad 'true';

--LOAD BUSINESS DATA
buinessData = LOAD '/user/kx361/business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as (json:map[]);

--GET THE REQUIRED DATA 
requiredData = 	FOREACH buinessData GENERATE FLATTEN(json#'categories') AS type_of_business, (float)json#'stars' AS stars, (float)json#'latitude' as latitude, (float)json#'longitude' as longitude;
                            
--FILTER THE DATA BY LINGITUDE AND LATITUDE
wisconsinBusiness = 	FILTER requiredData BY (latitude>42.9083) AND (latitude<43.2417) AND (longitude>-89.5839) AND (longitude<-89.2506);

-- GROUP THE DATA BY BUSINESS TYPE
groupedByCategory = GROUP wisconsinBusiness BY type_of_business;

-- FOR EACH GROUP, GENERATE THE AVG NUMBER OF STARS
finalData = FOREACH groupedByCategory GENERATE group as categories, AVG(wisconsinBusiness.stars) AS stars;

--FINALLY SORT THE DATA BY CATEGORIES/TYPE
orderedData = ORDER finalData BY categories;
										
--STORE IT AS CSV
STORE orderedData INTO './p3' using PigStorage(',');