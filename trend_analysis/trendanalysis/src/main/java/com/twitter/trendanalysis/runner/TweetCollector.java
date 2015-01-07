package com.twitter.trendanalysis.runner;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.twitter.trendanalysis.model.Constants;
import com.twitter.trendanalysis.util.TweetsToFile;

import java.net.UnknownHostException;


public class TweetCollector {

    public static void main(String[] args) {
        try {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            DB db = mongoClient.getDB("twitter_sampling");
            DBCollection tweetCollection = db.getCollection("tweet_collection");

            collectPsnHackTweets(db, tweetCollection);
            collectAirAsiaTweets(db, tweetCollection);
            collectHolidayTweets(db, tweetCollection);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void collectPsnHackTweets(DB db, DBCollection tweetCollection) {
        DBCollection psnHackCollection = db.getCollection(Constants.PSN_HACK_TWEET_COLLECTION);
        psnHackCollection.drop();
        psnHackCollection = db.createCollection(Constants.PSN_HACK_TWEET_COLLECTION, null);
        BasicDBList or = new BasicDBList();

        or.add(getHashTagQuery("psn"));
        or.add(getHashTagQuery("lizardsquad"));
        or.add(getHashTagQuery("playstationnetwork"));
        or.add(getHashTagQuery("psndown"));
        or.add(getHashTagQuery("finestsquad"));
        or.add(getHashTagQuery("lizardpatrol"));
        or.add(getHashTagQuery("payingfornothing"));
        or.add(getHashTagQuery("psndowntime"));
        or.add(getHashTagQuery("playstationsucks"));
        or.add(getHashTagQuery("xboxlivedown"));
        or.add(getHashTagQuery("xboxlive"));
        or.add(getHashTagQuery("psnup"));

        or.add(getUserMentionsQuery("PlayStation"));
        or.add(getUserMentionsQuery("KimDotcom"));
        or.add(getUserMentionsQuery("MEGAprivacy"));
        or.add(getUserMentionsQuery("AskPlayStation"));
        or.add(getUserMentionsQuery("LizardMafia"));
        or.add(getUserMentionsQuery("XboxSupport"));
        or.add(getUserMentionsQuery("Xbox"));

        BasicDBObject query = new BasicDBObject("$or", or);
        Cursor cursor = tweetCollection.find(query);
        try {
            int i = 0;
            while (cursor.hasNext()) {
                i++;
                psnHackCollection.insert(cursor.next());
            }
            System.out.println("Tweets for psn hack: " + i);
        } finally {
            cursor.close();
        }

        TweetsToFile.saveTweetsToFile(Constants.PSN_HACK_TWEET_COLLECTION);
    }

    public static void collectAirAsiaTweets(DB db, DBCollection tweetCollection) {
        DBCollection airAsiaCollection = db.getCollection(Constants.AIR_ASIA_TWEET_COLLECTION);
        airAsiaCollection.drop();
        airAsiaCollection = db.createCollection(Constants.AIR_ASIA_TWEET_COLLECTION, null);
        BasicDBList or = new BasicDBList();

        or.add(getHashTagQuery("airasia"));
        or.add(getHashTagQuery("qz8501"));
        or.add(getHashTagQuery("prayforairasia"));
        or.add(getHashTagQuery("prayforqz8501"));
        or.add(getHashTagQuery("airasia8501"));
        or.add(getHashTagQuery("mh370"));

        or.add(getUserMentionsQuery("AirAsia"));

        BasicDBObject query = new BasicDBObject("$or", or);
        Cursor cursor = tweetCollection.find(query);
        try {
            int i = 0;
            while (cursor.hasNext()) {
                i++;
                airAsiaCollection.insert(cursor.next());
            }
            System.out.println("Tweets for air asia: " + i);
        } finally {
            cursor.close();
        }

        TweetsToFile.saveTweetsToFile(Constants.AIR_ASIA_TWEET_COLLECTION);
    }

    public static void collectHolidayTweets(DB db, DBCollection tweetCollection) {
        DBCollection holidayCollection = db.getCollection(Constants.HOLIDAY_TWEET_COLLECTION);
        holidayCollection.drop();
        holidayCollection = db.createCollection(Constants.HOLIDAY_TWEET_COLLECTION, null);
        BasicDBList or = new BasicDBList();

        or.add(getHashTagQuery("newyear"));
        or.add(getHashTagQuery("newyearseve"));
        or.add(getHashTagQuery("nye"));
        or.add(getHashTagQuery("hny"));
        or.add(getHashTagQuery("goodbye2014"));
        or.add(getHashTagQuery("bye2014"));
        or.add(getHashTagQuery("nye2015"));
        or.add(getHashTagQuery("midnight"));
        or.add(getHashTagQuery("ihatenewyears"));
        or.add(getHashTagQuery("newyearseveproblems"));
        or.add(getHashTagQuery("newyear2015"));
        or.add(getHashTagQuery("happynewyear"));
        or.add(getHashTagQuery("hello2015"));
        or.add(getHashTagQuery("welcome2015"));
        or.add(getHashTagQuery("hi2015"));
        or.add(getHashTagQuery("newyears"));

        BasicDBObject query = new BasicDBObject("$or", or);
        Cursor cursor = tweetCollection.find(query);
        try {
            int i = 0;
            while (cursor.hasNext()) {
                i++;
                holidayCollection.insert(cursor.next());
            }
            System.out.println("Tweets for holidays: " + i);
        } finally {
            cursor.close();
        }

        TweetsToFile.saveTweetsToFile(Constants.HOLIDAY_TWEET_COLLECTION);
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
