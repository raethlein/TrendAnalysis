package com.twitter.trendanalysis.util;

import java.io.File;
import java.io.FileWriter;
import java.net.UnknownHostException;

import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class TweetsToFile {

    public static void main(String[] args) throws Exception {
        try {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            DB db = mongoClient.getDB("twitter_sampling");
            DBCollection coll = db.getCollection("test_output");

            Cursor cursor = coll.find();
            File file = new File("src\\main\\resources\\test.txt");
            FileWriter fw = new FileWriter(file);
            try {
                int i = 0;                
                while (cursor.hasNext()) {
                    i++;
                    DBObject obj = cursor.next();
                    String tweet = "" + obj.get("text");
                    fw.write(TweetCleaner.cleanTweet(tweet) + "\n");
                    System.out.println(obj.get("text"));
                }
                System.out.println("Result count: " + i);
            } finally {
                cursor.close();
                fw.flush();
                fw.close();
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
