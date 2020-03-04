package twitterproject;

import com.mongodb.Block;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.bson.Document;

import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Savvas
 */
public class AnalyseTweets {
    
    private  ArrayList< HashMap<String, Double> > emotionalWords = null;
    private  ArrayList< HashMap<String, Double> > emoticons = null;
    MongoDatabase db = null;

    /**
     * Creates an object of the class by getting the emotionalWords list.
     */
    public AnalyseTweets(MongoDatabase aDb) {
        this.emotionalWords = CreateDictionaries.getEmotionalWords();
        this.emoticons = CreateDictionaries.getEmoticons();
        this.db = aDb;
    }
    
    /**
     * This method updates the "trends" collection so as to complete any missing data from it.
     * It reads the "tweets" collection and makes sure that every trend found there, also exists in the "trends" collection.
     * It also updates the start and end times of a trend if necessary, so that a trend will never end before a tweet has been returned for this trend.
     */
    public void completeMissingData() {
        FindIterable<Document> iterable = db.getCollection("tweets").find();
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document tweet) {
                FindIterable<Document> trendQuery = db.getCollection("trends").find(new Document("text", tweet.get("relatedTrend")));
                Document trend = trendQuery.first();
                
                //get tweet time.
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis( Long.parseLong( tweet.get("timestamp_ms").toString()));
                Date tweetTime = cal.getTime();

                if(trend == null) { //write new trend
                    Document doc = new Document().append("text", tweet.get("relatedTrend")).append("start_time", tweetTime).append("end_time", tweetTime);
                    db.getCollection("trends").insertOne(doc); //if not exists, put in database.
                }
                else {  //trend exists, see if start or end times need update.
                    
                    if(trend.get("start_time") == null) { //start_time has not been set. Set it now
                        db.getCollection("trends").updateOne(new Document("text", tweet.get("relatedTrend")), new Document("$set", new Document("start_time", tweetTime)));//if trend exists, update the endTime.
                    }
                    else {
                        cal = Calendar.getInstance();
                        Date trendTime = (Date)trend.get("start_time");
                        
                        if(trendTime.after(tweetTime)) {    //start_time of trend is after time of tweet, update the trend's start_time.
                            db.getCollection("trends").updateOne(new Document("text", tweet.get("relatedTrend")), new Document("$set", new Document("start_time", tweetTime)));//update the startTime.
                        }
                    }
                    
                    if(trend.get("end_time") == null) { //end_time has not been set. Set it now
                        db.getCollection("trends").updateOne(new Document("text", tweet.get("relatedTrend")), new Document("$set", new Document("end_time", tweetTime)));//if trend exists, update the endTime.
                    }
                    else {
                        cal = Calendar.getInstance();
                        Date trendTime = (Date)trend.get("end_time");

                        if(trendTime.before(tweetTime)) {    //end_time of trend is before time of tweet, update the trend's end_time.
                            db.getCollection("trends").updateOne(new Document("text", tweet.get("relatedTrend")), new Document("$set", new Document("end_time", tweetTime)));//if trend exists, update the endTime.
                        }
                    }
                }
            }
        });
    }
    
    
    /**
     * Reads all the tweets from MongoDB and updates the fields with their scores.
     */
    public void computeScores() {
        
        FindIterable<Document> iterable = db.getCollection("tweets").find();
        iterable.forEach(new Block<Document>() {
            
            @Override 
            /*
             * First split the tweet and remove links, RT, mentions etc. Then append the remaining words, stem and split again, but this time throw any non-word character.
             */
            public void apply(final Document document) {
                String tweet = document.get("text").toString().toLowerCase();
                List<String> tweetWords = new LinkedList<>(java.util.Arrays.asList(tweet.split(" "))); //get text of a tweet, split into words and put into list.

                for (Iterator<String> it = tweetWords.iterator(); it.hasNext();) {
                    String word = it.next();
                    if(word.length()==0 || word.contains("@") || word.contains("https://") || word.contains("http://") || word.equals("RT")) {
                        it.remove();
                    }
                }

                StringBuilder ntw = new StringBuilder(); //ntw is the full text of the tweet after removing links, mentions etc and before stemming.

                for (String string : tweetWords) {
                    ntw.append(string).append(" ");
                }
                if(ntw.length()>0) {
                    ntw.deleteCharAt((int)(ntw.length()-1)); //remove the last " " that was appended.
                }
                
                String stemmed = PorterStemmer.getStemmedSentence(ntw.toString());  //stem tweet.
                
                tweetWords = new LinkedList<>(java.util.Arrays.asList(stemmed.split("\\W")));    // \W means any non-word character. (Word characters: [a-zA-Z_0-9])
               
                double[] scores = new double[6];
                int[] scoresCount = new int[6];

                //for every word in the tweet, get its score in the six feelings.
                for (String string : tweetWords) {
                    for (int i=0; i<emotionalWords.size(); i++) {
                        Double score = emotionalWords.get(i).get(string);
                        if(score != null) {
                            scores[i] += score;
                            scoresCount[i]++;
                        }
                    }
                }
                
                //for every emoticon, check if it exists inside the body of the tweet. checking the full tweet because the regex used in splitting deletes any non-word character.
                for (int i=0; i<emoticons.size(); i++) {
                    for (String emoti : emoticons.get(i).keySet()) {    //get all the emoticons for a feeling.
                        if(ntw.toString().contains(emoti)){     //if it is inside the tweet, add its score in the total score for this feeling.
                            scores[i] += emoticons.get(i).get(emoti);
                            scoresCount[i]++;
                        }
                    }
                }
                
                int emotionalWordsInTweet = 0;
                for (int i = 0; i < scoresCount.length; i++) {
                    emotionalWordsInTweet += scoresCount[i];
                }
                
                for (int i = 0; i < scores.length; i++) {
                    if(emotionalWordsInTweet>0) {
                        scores[i] = scores[i]/emotionalWords.get(i).size()/emotionalWordsInTweet;
                    }
                    else {
                        scores[i] = 0;
                    }
                }
                
                Counter.increment();
                if(Counter.getCounter()%10000==0) {
                    System.out.println("tweets processed: " + Counter.getCounter());
                }
                
                //put the scores in mongo collection.
                Document update = new Document("formattedText", stemmed)
                                            .append("angerScore", scores[0])
                                            .append("disgustScore", scores[1])
                                            .append("fearScore", scores[2])
                                            .append("joyScore", scores[3])
                                            .append("sadnessScore", scores[4])
                                            .append("surpriseScore", scores[5]);
                
                db.getCollection("tweets").updateOne(document, update);
           }
        });
        
    }
    
    
    /**
     * For this method to work properly, the scores need to have been computed before.
     * Also, it will need consistence in collections "trends" and "tweets", since it gets
     * data from both. So, it is better to run the method that completes all missing data from "trends" before calling this method.
     * 
     * This method will create a new collection named "statistics" in the database. It will contain statistical info for every trend recorded in the base.
     * For each trend, all the related tweets will be gathered in an array sorted by their timestamp and then split in intervals depending on the total duration of the trend, average scores will be extracted.
     * This info gets stored in "statistics" collection as an array, where each element of the array has info for one timeframe.
     */    
    public void AnalyzeStatistics() {
        
        int totalTrends = 1170; //it is hardcoded but there is no problem because hashmap can expand on its own, this is just a starting size.
        
        final HashMap< String, Pair<Date, Integer> > trendTimeInfo = new HashMap<>(totalTrends);
        final HashMap< String, ArrayList<TimeFrame> > trendStats = new HashMap<>(totalTrends);
        
        FindIterable<Document> trendsQuery = db.getCollection("trends").find().sort(new Document("text", 1));
        
        
        trendsQuery.forEach(new Block<Document>(){

            @Override
            public void apply(Document t) {
                
                //get active time of trend.
                Date startTime = (Date) t.get("start_time");
                Date endTime = (Date) t.get("end_time");

                //calculate timeframes.
                int intervalDurationMinutes = 15;
                long diffInSeconds = (long) Math.ceil((endTime.getTime() - startTime.getTime())/1000); //get the time difference in seconds between the first and the last tweet for this trend.
                int diffInMinutes = (int) Math.ceil(diffInSeconds/60.0);
                if(diffInMinutes<300) {
                    intervalDurationMinutes = 15;
                }
                else if(diffInMinutes<600) {
                    intervalDurationMinutes = 30;
                }
                else if(diffInMinutes<1200) {
                    intervalDurationMinutes = 60;
                }
                else {
                    intervalDurationMinutes = 120;
                }
                int totalTimeFrames = (int) Math.max(1, Math.ceil(diffInSeconds / (60.0*intervalDurationMinutes)));   //divide the total time duration in equal frames. Needs to be at least 1 timeframe.
                
                //add info about trend and create the arraylist for its timeframes.
                trendTimeInfo.put(t.get("text").toString(), new Pair<>(startTime, intervalDurationMinutes));
                
                ArrayList<TimeFrame> emptyTimeframes = new ArrayList<>(totalTimeFrames);
                for (int i = 0; i < totalTimeFrames; i++) {
                    emptyTimeframes.add(new TimeFrame());
                }
                
                trendStats.put(t.get("text").toString(), emptyTimeframes);
            }
        });
        
        
        FindIterable<Document> tweetsQuery = db.getCollection("tweets").find();
        
        tweetsQuery.forEach(new Block<Document>() {

            @Override
            public void apply(Document t) {
                Counter.increment();
                if(Counter.getCounter()%10000==0) {
                    System.out.println("tweets processed: " + Counter.getCounter());
                }
                
                Pair<Date, Integer> trendTimes = trendTimeInfo.get(t.get("relatedTrend").toString());
                
                Date trendStartTime = trendTimes.getKey();
                Date tweetTime = new Date(Long.parseLong(t.get("timestamp_ms").toString()));
                
                long diffInSeconds = (long) Math.ceil((tweetTime.getTime() - trendStartTime.getTime())/1000); //get the time difference in seconds between the first and the last tweet for this trend.
                int diffInMinutes = (int) Math.ceil(diffInSeconds/60.0);
                
                double timeframeIndex = 1.0 * diffInMinutes / trendTimes.getValue();
                int timeframeIndexInt = (int) timeframeIndex;
                
                //if the index is equal to an integer, then go to the previous timeframe. This is because we want timeframes to be inclusive on endtime, so if a tweet is exactly at a frame's end, it must go in this frame and not in the next.
                if(timeframeIndexInt > 0) {
                    if(timeframeIndex == Math.floor(timeframeIndex)) {
                        timeframeIndexInt--;
                    }
                }
                
                TimeFrame timeframe = trendStats.get(t.get("relatedTrend")).get(timeframeIndexInt);
                timeframe.increaseAngerAvg(Double.parseDouble(t.get("angerScore").toString()));
                timeframe.increaseDisgustAvg(Double.parseDouble(t.get("disgustScore").toString()));
                timeframe.increaseFearAvg(Double.parseDouble(t.get("fearScore").toString()));
                timeframe.increaseJoyAvg(Double.parseDouble(t.get("joyScore").toString()));
                timeframe.increaseSadnessAvg(Double.parseDouble(t.get("sadnessScore").toString()));
                timeframe.increaseSurpriseAvg(Double.parseDouble(t.get("surpriseScore").toString()));
                
                String[] tweetWords = t.get("formattedText").toString().split("\\W");
                    
                for (int j = 0; j < 6; j++) {
                    for (int k = 0; k < tweetWords.length; k++) {
                        if(emotionalWords.get(j).containsKey(tweetWords[k])) {
                            timeframe.incrementWordFrequency(j, tweetWords[k]);
                        }
                    }
                }
            }
        });
        
        
        List< Map.Entry<String, ArrayList<TimeFrame>> > trendStatsSorted = new LinkedList<>(trendStats.entrySet());
        
        Collections.sort(trendStatsSorted, new Comparator(){

            @Override
            public int compare(Object o1, Object o2) {
                Map.Entry<String, ArrayList<TimeFrame>> obj1 = (Map.Entry<String, ArrayList<TimeFrame>>)o1;
                Map.Entry<String, ArrayList<TimeFrame>> obj2 = (Map.Entry<String, ArrayList<TimeFrame>>)o2;
                
                return obj1.getKey().compareTo(obj2.getKey());
            }
            
        });
        
        
        int trendCount = 0;
        
        for (Map.Entry<String, ArrayList<TimeFrame>> trend : trendStatsSorted) {
            
            System.out.println("TrendCount: " + ++trendCount + " trend:" + trend.getKey());
            
            int totalTimeframes = trend.getValue().size();
            int totalTweets = 0;
            for (int i = 0; i < totalTimeframes; i++) {
                totalTweets += trend.getValue().get(i).getTweetCount();
            }
            
            Document trendDocumentToWrite = new Document("trendName", trend.getKey()).append("totalTweets", totalTweets).append("totalTimeframes", totalTimeframes);
            
            List<Document> timeframes = new ArrayList<>(totalTimeframes);

            for (int i = 0; i < totalTimeframes; i++) {
                TimeFrame timeframe = trend.getValue().get(i);
                
                Document singleFrame = new Document("numOfTweets", timeframe.getTweetCount())
                                                    .append("angerAvg", timeframe.getAngerAvg())
                                                    .append("disgustAvg", timeframe.getDisgustAvg())
                                                    .append("fearAvg", timeframe.getFearAvg())
                                                    .append("joyAvg", timeframe.getJoyAvg())
                                                    .append("sadnessAvg", timeframe.getSadnessAvg())
                                                    .append("surpriseAvg", timeframe.getSurpriseAvg());
                
                List<Map.Entry<String, Integer>> wordsTF = timeframe.getWordsTF();
                List<Document> documentTfValues = new ArrayList<>(wordsTF.size());
                for (int j = 0; j < wordsTF.size(); j++) {
                    Document wordFrequency = new Document("word", wordsTF.get(j).getKey()).append("tf", wordsTF.get(j).getValue());
                    documentTfValues.add(wordFrequency);
                }
                singleFrame.append("tfValues", documentTfValues);
                
                timeframes.add(singleFrame);
            }
            trendDocumentToWrite.append("timeframes", timeframes);
            db.getCollection("statistics").insertOne(trendDocumentToWrite);
        }
    }
}
