package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created By: Melanie Baybay
 * Last Modified: 10/14/17
 */
public class PortManager {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private int first = 14000;
    private int last = 15000;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public int getAvailablePort() {
        int attempted = 0;
        int port = ThreadLocalRandom.current().nextInt(first, last);
        while(attempted < last) {
            try {
                attempted += 1;
                new ServerSocket(port).close();
                return port;
            } catch (IOException ignore) {
            }
        }
        return -1;
    }
    /** ------------------------------------------------------------------------------------- **/
}
