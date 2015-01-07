package com.twitter.trendanalysis.runner;

import java.net.UnknownHostException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class TweetCollector {

    public static void main(String[] args) {
        try {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            DB db = mongoClient.getDB("twitter_sampling");
            DBCollection coll = db.getCollection("tweet_collection");
            DBCollection testColl = db.createCollection("test_output", null);

            BasicDBObject hashtagElemMatch = new BasicDBObject("text", "Job");
            BasicDBObject hashtagMatch = new BasicDBObject("$elemMatch", hashtagElemMatch);
            BasicDBObject hashtagQuery = new BasicDBObject("entities.hashtags", hashtagMatch);

            BasicDBObject usermentionsElemMatch = new BasicDBObject("screen_name", "meghnlopez");
            BasicDBObject usermentionsMatch = new BasicDBObject("$elemMatch", usermentionsElemMatch);
            BasicDBObject usermentionsQuery = new BasicDBObject("entities.user_mentions", usermentionsMatch);

            BasicDBList or = new BasicDBList();
            or.add(hashtagQuery);
            or.add(usermentionsQuery);

            BasicDBObject query = new BasicDBObject("$or", or);
            Cursor cursor = coll.find(query);
            try {
                int i = 0;
                while (cursor.hasNext()) {
                    i++;
                    testColl.insert(cursor.next());
                }
                System.out.println("Result count: " + i);
            } finally {
                cursor.close();
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
