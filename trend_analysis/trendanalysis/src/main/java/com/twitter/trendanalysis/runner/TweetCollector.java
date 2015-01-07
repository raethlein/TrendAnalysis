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
            
            BasicDBList or = new BasicDBList();
            
            or.add(getHashTagQuery("psn"));
            or.add(getHashTagQuery("lizardquad"));
            or.add(getHashTagQuery("playstationnetwork"));
            or.add(getHashTagQuery("psndown"));
            or.add(getHashTagQuery("finestsquad"));
            or.add(getHashTagQuery("lizardpatrol"));
            or.add(getHashTagQuery("payingfornothing"));
            or.add(getHashTagQuery("psndowntime"));
            or.add(getHashTagQuery("playstationsucks"));
            or.add(getHashTagQuery("xboxlivedown"));
            or.add(getHashTagQuery("xboxsupport"));
            or.add(getHashTagQuery("xbl"));
            or.add(getHashTagQuery("psnup"));
            
            or.add(getUserMentionsQuery("PlayStation"));
            or.add(getUserMentionsQuery("KimDotcom"));
            or.add(getUserMentionsQuery("Megaprivacy"));
            or.add(getUserMentionsQuery("AskPlayStation"));
            or.add(getUserMentionsQuery("LizardMafia"));

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
    
    public static BasicDBObject getHashTagQuery(String hashtag) {
        BasicDBObject hashtagElemMatch = new BasicDBObject("text", hashtag);
        BasicDBObject hashtagMatch = new BasicDBObject("$elemMatch", hashtagElemMatch);
        return new BasicDBObject("entities.hashtags", hashtagMatch);
    }
    
    public static BasicDBObject getUserMentionsQuery(String screenName) {
        BasicDBObject usermentionsElemMatch = new BasicDBObject("screen_name", screenName);
        BasicDBObject usermentionsMatch = new BasicDBObject("$elemMatch", usermentionsElemMatch);
        return new BasicDBObject("entities.user_mentions", usermentionsMatch);
    }
}
