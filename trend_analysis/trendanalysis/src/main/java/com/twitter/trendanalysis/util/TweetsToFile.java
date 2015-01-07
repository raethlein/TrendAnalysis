package com.twitter.trendanalysis.util;

import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.io.File;
import java.io.FileWriter;


public class TweetsToFile {

    public static void saveTweetsToFile(String collectionName) {
        try {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            DB db = mongoClient.getDB("twitter_sampling");
            DBCollection coll = db.getCollection(collectionName);

            Cursor cursor = coll.find();
            String fileName = collectionName + ".txt";
            File file = new File("src\\main\\resources\\" + fileName);
            FileWriter fw = new FileWriter(file);
            try {
                int i = 0;
                while (cursor.hasNext()) {
                    i++;
                    DBObject obj = cursor.next();
                    String tweet = "" + obj.get("text");
                    fw.write(TweetCleaner.cleanTweet(tweet) + "\n");
                }
                System.out.println("Tweets saved to file: " + i);
            } finally {
                cursor.close();
                fw.flush();
                fw.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
