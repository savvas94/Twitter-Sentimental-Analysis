/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterproject;

/**
 *
 * @author Savvas
 */
public class Counter {
    
    private static long counter = 0;
    
    public static void increment() {
        counter++;
    }
    
    public static long getCounter() {
        return counter;
    }
    
}
