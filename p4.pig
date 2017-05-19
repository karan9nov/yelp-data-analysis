SET elephantbird.jsonloader.nestedLoad 'true';

--Fetch business data
businessData = LOAD './business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (yelp_business: map[]);
business = FOREACH businessData GENERATE yelp_business#'business_id' as business_id, yelp_business#'categories' as categories; 

--Fetch review data
reviewData = LOAD './yelp_academic_dataset_review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (yelp_review: map[]);
review = FOREACH reviewData GENERATE yelp_review#'user_id' as user_id1, yelp_review#'business_id' as business_id, (int)yelp_review#'stars' as stars;

--Fetch user data
userData = LOAD './yelp_academic_dataset_user.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (yelp_user: map[]);
user = FOREACH userData GENERATE yelp_user#'user_id' as user_id, (int)yelp_user#'review_count' as review_count;

--Sort the order in descending order
sortedUsers = ORDER user BY review_count DESC;

--Get the top 10
top10Users = LIMIT sortedUsers 10;

--Join the users with reviews on by user id
userReviewJoined = JOIN top10Users BY user_id, review BY user_id1;

--Jooin the joined part with businesses
userReviewBusinessJoined = JOIN business BY business_id, userReviewJoined BY business_id;

--Flatten the data
flattenedData = FOREACH userReviewBusinessJoined GENERATE FLATTEN(categories), stars, user_id;

--group the data by users and also categories
grouped = GROUP flattenedData BY (user_id, categories);

--calculate the averaage and sort it
grouped_result = FOREACH grouped GENERATE group, AVG(flattenedData.stars) as avg_stars; 
ordered_result = ORDER grouped_result BY user_id;

--store the result into the directory
STORE grouped_result INTO './p4' using PigStorage(',');
