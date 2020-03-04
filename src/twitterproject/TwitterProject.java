/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.DeleteResult;
import java.awt.Cursor;
import twitter4j.Trend;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.bson.Document;
import static java.util.Arrays.asList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.types.ObjectId;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

/**
 * @author savvaspc
 */
public class TwitterProject {

    private static MongoDatabase db = null;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws TwitterException, MalformedURLException, IOException, InterruptedException {
        
        MongoClient mongoClient = new MongoClient();

        final MongoDatabase db = mongoClient.getDatabase("twitterProject");
        
        analyzeStatistics();
    }
    
    private static void collectData() {
        //Your Twitter App's Consumer Key savvas
        String consumerKey = "CmunEvvpVFEvbPfuQqBMEJOFd";

        //Your Twitter App's Consumer Secret
        String consumerSecret = "KqXGva9KpuMNAg4tAxiwruU9cVCYGxjBaiNuMrQooAjy3rGsVx";

        //Your Twitter Access Token
        String accessToken = "590136061-o59LGThDDf91jXMR2x6rc9rQWEaBFzlAFgscFjsp";

        //Your Twitter Access Token Secret
        String accessTokenSecret = "oyHA3IW2X7orxzekws0McbzyQCxjAAj3XUAzIg1bdGWXE";


        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setOAuthConsumerKey(consumerKey);
        builder.setOAuthConsumerSecret(consumerSecret);
        builder.setOAuthAccessToken(accessToken);
        builder.setOAuthAccessTokenSecret(accessTokenSecret);
        builder.setJSONStoreEnabled(true);
        Configuration configuration = builder.build();

        //Instantiate a new Twitter instance
        Twitter twitter = new TwitterFactory(configuration).getInstance();

        Object lock = new Object();
        HashMap<Trend, Pair<Date, Date>> activeTrends = new HashMap<Trend, Pair<Date, Date>>(60);

        new TrendsReader(lock, activeTrends, twitter, configuration, db).start();
    }
    
    private static void analyzeStatistics() {
        AnalyseTweets x = new AnalyseTweets(db); 
        
        x.completeMissingData();
        
        x.computeScores();
        
        x.AnalyzeStatistics();
        
    }
}