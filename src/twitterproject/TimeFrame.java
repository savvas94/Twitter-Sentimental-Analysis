/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import java.util.ArrayList;
import static java.util.Arrays.sort;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Savvas
 */
public class TimeFrame {
    
    private int tweetCount;
    private double angerAvg;
    private double disgustAvg;
    private double fearAvg;
    private double joyAvg;
    private double sadnessAvg;
    private double surpriseAvg;
    private HashMap<String, Integer>[] wordsTF;

    public TimeFrame() {
        tweetCount = 0;
        angerAvg = 0;
        disgustAvg = 0;
        fearAvg = 0;
        joyAvg = 0;
        sadnessAvg = 0;
        surpriseAvg = 0;
        wordsTF = new HashMap[6];
        for (int i = 0; i < wordsTF.length; i++) {
            wordsTF[i] = new HashMap<>();
        }
    }
    
    /**
     * Increases the word count for the specified word in the specified feeling by 1.
     * 
     * @param feeling the feeling on which the word belongs.
     * @param word the word that will be increased.
     */
    public void incrementWordFrequency(int feeling, String word) {
        if(wordsTF[feeling].containsKey(word)) {
            wordsTF[feeling].put(word, wordsTF[feeling].get(word) + 1);
        }
        else {
            wordsTF[feeling].put(word, 1);
        }
    }
    
    /*
     * Returns the representive words for the strongest feeling along with their tf scores. The list is returned sorted.
     */
    public List<Map.Entry<String, Integer>> getWordsTF() {
        
        //find the strongest feeling and its index. 
        Integer[] scoresIndices = {0, 1, 2, 3, 4, 5};
        final Double[] scores = {angerAvg, disgustAvg, fearAvg, joyAvg, sadnessAvg, surpriseAvg};
        
        int maxPos = -1;
        double maxScore = -1;
        for (int i = 0; i < scores.length; i++) {
            if(scores[i]>maxScore) {
                maxScore = scores[i];
                maxPos = i;
            }
        }
        
        ArrayList< Map.Entry<String, Integer> > wordsSorting = new ArrayList<>(wordsTF[maxPos].entrySet());
        
        Collections.sort(wordsSorting, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                Map.Entry<String, Integer> obj1 = (Map.Entry<String, Integer>)o1;
                Map.Entry<String, Integer> obj2 = (Map.Entry<String, Integer>)o2;

                return -Integer.compare(obj1.getValue(), obj2.getValue());
            }
        });
        
        //for the feeling with the best score, get the tf of its emotional words.
        return new LinkedList<>(wordsSorting);
    }

    public double getAngerAvg() {
        return angerAvg/tweetCount;
    }

    public void increaseAngerAvg(double angerAvg) {
        tweetCount++; //increase the num of tweets (here we rely on good usage by user, but we are the users so no prob!)
        this.angerAvg += angerAvg;
    }

    public double getDisgustAvg() {
        return disgustAvg/tweetCount;
    }

    public void increaseDisgustAvg(double disgustAvg) {
        this.disgustAvg += disgustAvg;
    }

    public double getFearAvg() {
        return fearAvg/tweetCount;
    }

    public void increaseFearAvg(double fearAvg) {
        this.fearAvg += fearAvg;
    }

    public double getJoyAvg() {
        return joyAvg/tweetCount;
    }

    public void increaseJoyAvg(double joyAvg) {
        this.joyAvg += joyAvg;
    }

    public double getSadnessAvg() {
        return sadnessAvg/tweetCount;
    }

    public void increaseSadnessAvg(double sadnessAvg) {
        this.sadnessAvg += sadnessAvg;
    }

    public double getSurpriseAvg() {
        return surpriseAvg/tweetCount;
    }

    public int getTweetCount() {
        return tweetCount;
    }

    public void increaseSurpriseAvg(double surpriseAvg) {
        this.surpriseAvg += surpriseAvg;
    }
    
}
