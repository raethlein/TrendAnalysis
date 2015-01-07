package com.twitter.trendanalysis.util;


public class TweetCleaner {
    
    public static String cleanTweet(String tweet) {
        String cleaned = tweet;
        
        cleaned = cleaned.replace("\n", "");
        cleaned = cleaned.replace("\r", "");
        cleaned = cleaned.replace("\r\n", "");
        cleaned = cleaned.replace("'", "");
        cleaned = cleaned.replace("\"", "");
        
        cleaned = cleaned.replaceAll("https?://\\S+\\s?", "");
        cleaned = cleaned.replaceAll("[^A-Za-z\\s]", " ");        
        
        return cleaned;
    }
}
