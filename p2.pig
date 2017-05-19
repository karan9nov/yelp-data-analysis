SET elephantbird.jsonloader.nestedLoad 'true';

--LOAD BUSINESS DATA
yelp_business_data = LOAD '/user/kx361/business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as (json:map[]);

--FLATTEN OUT THE CATEGORIES
yelp_business_data_category = FOREACH yelp_business_data GENERATE (float)json#'stars' AS stars, json#'city' as city, FLATTEN(json#'categories') AS categories;

--GROUP BY CATEGORIES AND CITY
yelp_business_group= GROUP yelp_business_data_category BY (categories,city); 
                            
--CALCULATE THE AVERAGE NUMBER OF STARS FOR EACH GROUP
yelp_business_group_data= FOREACH yelp_business_group GENERATE group.categories as category,group.city AS city, AVG(yelp_business_data_category.stars) AS stars;
                                                    
--GROUP AND ORDER THIS DATASET BY CATEGORIES
yelp_data_order= ORDER yelp_business_group_data BY category ASC;
grouped_categories = group yelp_data_order by category;

--NOW THE EACH CATEGORY CONSISTS CITY AND AVERAGE STARS. SO WE SORT THAT FOR EACH SINGLE CATEGORY. 
sorted_stars_data = FOREACH grouped_categories{
	sorted= ORDER yelp_data_order by stars desc;
	GENERATE FLATTEN(sorted);
};

--AND FINALLY STORE THE RESULT. 
STORE sorted_stars_data INTO './p2' using PigStorage(',') ;