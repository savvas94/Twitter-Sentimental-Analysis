/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.*;
import twitter4j.conf.Configuration;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * @author Savvas
 */
public class TrendsReader extends Thread {

    //a lock to synchronise reading and writing on the database.
    private final Object lock;

    //the active trends. this class writes trends in the hashmap and TweetsReader reads them.
    HashMap<Trend, Pair<Date, Date>> activeTrends;

    //a Twitter instance
    Twitter twitter;

    TweetsReader reader = null;
    Configuration conf = null;
    MongoDatabase db = null;

    private static long alt = 0;

    public TrendsReader(Object lock, HashMap<Trend, Pair<Date, Date>> activeTrends, Twitter aTwitter, Configuration aConf, MongoDatabase aDb) {
        this.lock = lock;
        this.activeTrends = activeTrends;
        this.twitter = aTwitter;
        this.conf = aConf;
        this.db = aDb;
    }


    @Override
    public void run() {
        //get starting time
        Calendar cal = Calendar.getInstance();

        //get finishing time
        cal.add(Calendar.DAY_OF_MONTH, 3);
        cal.add(Calendar.MINUTE, 1); //add 1 minute to make sure one last execution runs in the 3-hour duration.
        Date stopTime = cal.getTime();

        do {
            try {
                Trends trends = twitter.getPlaceTrends(1);

                cal = Calendar.getInstance();
                Date now = cal.getTime();
                cal.add(Calendar.HOUR_OF_DAY, 2);
                Date endDate = cal.getTime();

                for (Trend tr : trends.getTrends()) {

                    synchronized (lock) {

                        if (!activeTrends.containsKey(tr)) {
                            alt++;
                            activeTrends.put(tr, new Pair(now, endDate));
                            //Document doc = new Document().append("text", tr.getName()).append("start_time", now).append("end_time", endDate);
                            //db.getCollection("trends").insertOne(doc); //if not exists, put in database.
                        } else {
                            //Keep the original start date and update the end date
                            Date originalDate = activeTrends.get(tr).getKey();
                            activeTrends.put(tr, new Pair(originalDate, endDate));
                            //db.getCollection("trends").updateOne(new Document("text", tr.getName()), new Document("$set", new Document("end_time", endDate)));//if trend exists, update the endTime.
                        }

                    }
                }

                discardOutdatedTrendsAndWriteToDB();
                
                if (reader == null) { //start getting the tweets
                    TwitterStream stream = new TwitterStreamFactory(conf).getInstance();
                    reader = new TweetsReader(lock, activeTrends, stream, db);
                    reader.start();
                } else { //update stream filter
                    reader.updateSearchList();
                }

                System.out.println("INFO - " + now.toString() + " - Active trends right now: " + activeTrends.size());
                System.out.println("INFO - " + now.toString() + " - Total trends: " + alt);

                sleep(300000);

            } catch (TwitterException ex) {
                Logger.getLogger(TrendsReader.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(TrendsReader.class.getName()).log(Level.SEVERE, null, ex);
            }

            //reset cal to current time
            cal = Calendar.getInstance();
        }
        while (stopTime.after(cal.getTime())); //stop execution only when current time exceeds endtime
        
        //final two hours without new trends
        try {
            sleep(7200000);
        } catch (InterruptedException ex) {
            Logger.getLogger(TrendsReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        //discard all final trends
        discardOutdatedTrendsAndWriteToDB();
        
        //make sure all trends are gone
        activeTrends.clear();
        
        //close stream by updating it to an empty filter
        reader.updateSearchList();
        
        //shut down thread
        reader.stop();
    }


    private void discardOutdatedTrendsAndWriteToDB() {

        Date now = Calendar.getInstance().getTime();
        
        for (Iterator<Trend> it = activeTrends.keySet().iterator(); it.hasNext();) {
            
            Trend tr = it.next();
            Pair<Date, Date> dates = activeTrends.get(tr);
            if (dates.getValue().before(now)) { //Trend is no longer active
                //Find trend in db if it exists
                if (db.getCollection("trends").count(new Document("text", tr.getName())) > 0) {
                    //Update the trend
                    db.getCollection("trends").updateOne(new Document("text", tr.getName()), new Document("$set", new Document("end_time", dates.getValue())));//if trend exists, update the endTime.
                } else {
                    Document doc = new Document().append("text", tr.getName()).append("start_time", dates.getKey()).append("end_time", dates.getValue());
                    db.getCollection("trends").insertOne(doc); //if not exists, put in database.
                }

                synchronized (lock) {
                    it.remove(); //have to remove from iterator because otherwise ConcurrentModificationException
                }
            }
        }
    }
}
