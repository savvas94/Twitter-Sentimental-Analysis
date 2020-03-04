/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

import edu.smu.tspell.wordnet.Synset;
import edu.smu.tspell.wordnet.WordNetDatabase;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Savvas
 */
public class CreateDictionaries {
    
    private static ArrayList< HashMap<String, Double> > emotionalWords = null;
    
    /**
     * calls the function to read the polarities from senticnet, and also gets the synonyms of the initial emotional words. then it
     * joins the two lists into their intersection.
     */
    private static void createDictionary() {
        
        HashSet<String> stopWords = readStopWords();
        
        //read the polarities from senticnet.
        HashMap<String, Double> polarities = null;
        
        ArrayList< HashSet<String> > wordnetSynonyms = null;
        
        boolean polaritiesExist = true;
        try {
            
            polarities = readPolarities();
            
        } catch (FileNotFoundException ex) {
            System.err.println("Could not read the polarities file, so they will be ignored (set all polarities for existing words to 1.0).");
            polaritiesExist = false;
        }
        
        //read the synonyms from wordnet and expand the dictionary
        try {
            Scanner in = new Scanner(new File("secondary_emotions.txt"));
            
            ArrayList<String[]> initialWords = new ArrayList<>(6);
            
            while(in.hasNext()){
                String line = in.nextLine();
                initialWords.add(line.split("\t"));
            }
            
            wordnetSynonyms = new ArrayList<>(6);
            
            //get the 1st level of synonyms.
            for (int i = 0; i < initialWords.size(); i++) {
                wordnetSynonyms.add(i, new HashSet<String>());
                for (String word : initialWords.get(i)) {
                    wordnetSynonyms.get(i).addAll(getSynonyms(word));
                }
            }
            
            //get the 2nd level of synonyms.
            for (int i = 0; i < initialWords.size(); i++) {
                HashSet<String> temp = new HashSet<>();
                for (String word : wordnetSynonyms.get(i)) {
                    temp.addAll(getSynonyms(word));
                }
                wordnetSynonyms.get(i).addAll(temp);
            }
            
        } catch (FileNotFoundException ex) {    //Print error and return empty lists.
            System.err.println("Could not read the secondary-emotions.txt file, so they will be ignored (empty list is returned).");
            ArrayList< HashMap<String, Double> > emotionalWordsEmpty = new ArrayList<>(6);
            for (int i = 0; i < 6; i++) { //for every emotion
                emotionalWordsEmpty.add(i, new HashMap<String, Double>()); //create a hashmap
            }
            emotionalWords = emotionalWordsEmpty; //return the empty lists.
            return;
        }
        
        //the final list
        emotionalWords = new ArrayList<>(6);

        //create the list by joining the two lists.
        for (int i = 0; i < wordnetSynonyms.size(); i++) { //for every emotion
            
            emotionalWords.add(i, new HashMap<String, Double>()); //create a hashmap
            for (String word : wordnetSynonyms.get(i)) { //for every word in the old hashmap
                
                //check if the word exists in the polarities hashmap and does NOT exist in the stopwords hashset. if true, add it in the final dictionary.
                Double pol;
                if(polaritiesExist) { //check if there are polarities at all.
                    pol = polarities.get(word);
                }
                else {
                    pol = 1.0;
                }
                if(pol != null && !stopWords.contains(word)) {
                    if( (i==0 || i==1 || i==2 || i==4 ) && pol<0) { //for these 4 feelings, only keep negative words.
                        emotionalWords.get(i).put(PorterStemmer.getStemmedSentence(word), -pol); //stem the word before adding it in the dictionary. We need absolute value of polarity, so inverse (because we know it it negative).
                    }
                    else if(( i==3 || i==5) && pol>0) { //for these feelings, only keep positive words.
                        emotionalWords.get(i).put(PorterStemmer.getStemmedSentence(word), pol); //stem the word before adding it in the dictionary.
                    }
                }
            }
        }
    }
    
    
    /*
     * returns the synonyms found in wordnet for the specified word.
     */
    private static HashSet<String> getSynonyms(String word){
        
        System.setProperty("wordnet.database.dir", "C:\\Program Files (x86)\\WordNet\\2.1\\dict");
        WordNetDatabase database = WordNetDatabase.getFileInstance();
        
        
        //get synsets for specified word
        Synset[] synsets = database.getSynsets(word);
        
        
        HashSet<String> hs = new HashSet();

        for (int i = 0; i < synsets.length; i++){
           String[] wordForms = synsets[i].getWordForms();
             for (int j = 0; j < wordForms.length; j++)
             {
               hs.add(wordForms[j].toLowerCase());
             }
        }
        
        return hs;
    }
    
    
    /*
     * reads the senticnet3.rdf.xml file and returns a map with 30000 words and their polarities.
     */
    private static HashMap<String, Double> readPolarities() throws FileNotFoundException {
        
        Scanner in = new Scanner(new File("senticnet3.rdf.xml"));
        HashMap<String, Double> polarities = new HashMap<>(30000);

        while(in.hasNext()) {
            String line = in.nextLine();
            if(line.contains("rdf:about")){ //this is a line that starts an xml block and contains the name of its word.
                String[] temp = line.split("/");
                String word = temp[temp.length-1].substring(0, temp[temp.length-1].length()-2); //get the word by spliting the line and then getting the last string, and removing the 2 last characters of it.
                
                //read lines until reaching the polarity line.
                do {
                    line = in.nextLine();
                    if(line.contains("polarity xmlns")) {
                        
                        //split two times to seperate the number.
                        temp = line.split(">");
                        String[] temp2 = temp[temp.length-1].split("<");
                        
                        //put the pair in the hashmap
                        polarities.put(word, Double.parseDouble(temp2[0]));
                    }
                }
                while(!line.contains("polarity xmlns"));
                
            }
        }
        
        return polarities;
    }
    
    /*
     * reads the stopwords list and returns a set with all the words.
     */
    private static HashSet<String> readStopWords() {
        try {
            Scanner in = new Scanner(new File("stopwords.txt"));
            
            HashSet<String> words = new HashSet<>(640);
            
            while(in.hasNext()) {
                words.add(in.next());
            }
            
            return words;
        } catch (FileNotFoundException ex) {
            System.err.println("Could not read the stopwords file, so they will be ignored (empty lists is returned).");
        }
        return new HashSet<String>();
    }
    
    public static ArrayList< HashMap<String, Double> > getEmoticons() {
        
        ArrayList< HashMap<String, Double> > emoticons = new ArrayList<>(6);
            
        for (int i = 0; i < 6; i++) {
            emoticons.add(i, new HashMap<String, Double>());
        }
        
        try {
            Scanner in = new Scanner(new File("emoticons.txt"));    
            
            while(in.hasNext()) {
                String line = in.nextLine();
                String[] split = line.split("\t");
                if(split[0].equals("anger")) {
                    emoticons.get(0).put(split[3], Double.parseDouble(split[2]));
                }
                else if(split[0].equals("disgust")) {
                    emoticons.get(1).put(split[3], Double.parseDouble(split[2]));
                }
                else if(split[0].equals("fear")) {
                    System.out.println(line);
                    emoticons.get(2).put(split[3], Double.parseDouble(split[2]));
                }
                else if(split[0].equals("joy")) {
                    emoticons.get(3).put(split[3], Double.parseDouble(split[1]));
                }
                else if(split[0].equals("sadness")) {
                    emoticons.get(4).put(split[3], Double.parseDouble(split[2]));
                }
                else if(split[0].equals("surprise")) {
                    emoticons.get(5).put(split[3], Double.parseDouble(split[1]));
                }
            }
            
            return emoticons;
        } catch (FileNotFoundException ex) {
            System.err.println("Could not read the emoticons file, so they will be ignored (empty lists is returned).");
        }
        return emoticons;
        
    }

    public static ArrayList<HashMap<String, Double>> getEmotionalWords() {
        createDictionary();
        return emotionalWords;
    }
    
}
