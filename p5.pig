SET elephantbird.jsonloader.nestedLoad 'true';

--
business_data = LOAD './business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map []);

required_business_data = FOREACH business_data GENERATE json#'business_id' as business_id, json#'city' as city, json#'name' as name,(double) json#'latitude' as latitude, (float) json#'longitude' as longitude, (double) json#'stars' as stars, FLATTEN(json#'categories') as categories;

filtered_data = FILTER required_business_data BY (latitude <= 43.2467) AND (latitude >= 42.90833) AND (longitude >= -89.58389) AND (longitude <= -89.25056);

filtered_food_business = FILTER filtered_data by categories matches '.*Food.*';

business_id_stars = FOREACH filtered_food_business GENERATE business_id, stars;

ordered_data = ORDER business_id_stars by stars DESC;

top10 = LIMIT ordered_data 10;

Ordered_By_Stars_Asc = ORDER business_id_stars by stars;

bottom10 = LIMIT Ordered_By_Stars_Asc 10;

top10bottom10 = UNION top10, bottom10;

review_data = LOAD './yelp_academic_dataset_review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map []);

required_review_data = FOREACH review_data GENERATE json#'review_id' as review_id, json#'business_id' as business_id,json#'date' as date, json#'stars' as stars;

joined_reviews = JOIN required_review_data by business_id, top10bottom10 by business_id;

final_required_Data = FOREACH joined_reviews GENERATE top10bottom10::business_id as bid, (double) required_review_data::stars as star, required_review_data::review_id, SUBSTRING(required_review_data::date,5,7) as month;

filtered_data_by_month = FILTER final_required_Data BY (month matches '01|02|03|04|05');

grouped_data_by_business = GROUP filtered_data_by_month by bid;

avg_rating = FOREACH grouped_data_by_business GENERATE group, AVG(filtered_data_by_month.star) as avg_stars;

STORE avg_rating into './p5' using PigStorage (',');