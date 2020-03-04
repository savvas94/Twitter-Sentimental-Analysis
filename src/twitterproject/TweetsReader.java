/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.*;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

/**
 * @author Savvas
 */
public class TweetsReader extends Thread {

    //a lock to synchronise reading and writing on the database.
    private final Object lock;

    HashMap<Trend, Pair<Date, Date>> activeTrends;

    TwitterStream stream;
    private static long tls = 0;
    private static long alt = 0;
    MongoDatabase db = null;
    Random rand = null;
    StatusListener listener = null;

    public TweetsReader(Object lock, HashMap<Trend, Pair<Date, Date>> activeTrends, TwitterStream aStream, MongoDatabase aDb) {
        rand = new Random();
        this.lock = lock;
        this.activeTrends = activeTrends;
        this.stream = aStream;
        this.db = aDb;
    }

    @Override
    public void run() {

        listener = new StatusListener() {

            @Override
            public void onStatus(Status tweet) {

                if (tweet.getLang().equals("en")) {  //only keep tweets in english

                    //get json representation
                    String json = TwitterObjectFactory.getRawJSON(tweet);

                    Document ins = Document.parse(json);    //create a document for mongodb.

                    for (Trend trend : activeTrends.keySet()) {//find the trends that apply to this tweet.

                        if (tweet.getText().contains(trend.getName())) {

                            db.getCollection("tweets").insertOne(ins.append("relatedTrend", trend.getName()));

                            if (++alt % 100 == 0) {
                                System.out.println("INFO - " + Calendar.getInstance().getTime().toString() + " - " + alt + " tweets so far");
                            }
                        }
                    }
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                System.out.println("Deletion " + arg0.toString());
            }

            @Override
            public void onTrackLimitationNotice(int arg0) {
                if (++tls % 10000 == 0)
                    System.out.println("WARN - " + Calendar.getInstance().getTime().toString() + " - TrackLimitation " + tls);
            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                System.out.println("onScrubGeo");
            }

            @Override
            public void onStallWarning(StallWarning arg0) {
                System.out.println("onStallWarning: " + arg0.getMessage());
            }

            @Override
            public void onException(Exception excptn) {
                //System.out.println("onException " + excptn.getMessage());
            }
        };

        stream.addListener(listener);

        updateSearchList();
    }

    public void updateSearchList() {
        String[] keywords;

        //lock activeTrends because if an update happens during reading from it, there will be problems.
        synchronized (lock) {
            keywords = new String[activeTrends.size()];

            int i = 0;
            for (Trend trend : activeTrends.keySet()) {
                keywords[i++] = trend.getName();
            }
        }

        FilterQuery qry = new FilterQuery();

        qry.track(keywords);

        stream.filter(qry);
    }
}