package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created By: Melanie Baybay
 * Last Modified: 10/17/17
 */
public class ClientRetrievalData {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private final static Logger logger = Logger.getLogger(Client.class.getName());
    private CopyOnWriteArrayList<String> chunkIdList;
    private HashMap<String, byte[]> chunkMap;
    private ReentrantLock lock;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    public ClientRetrievalData(List<ByteString> chunkIds) {
        this.chunkIdList = convertChunkIdList(chunkIds);
        this.chunkMap = new HashMap<>();
        this.lock = new ReentrantLock();
    }
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public void put(String id, byte[] data) {
        logger.log(Level.INFO, "-- CHUNK RECIEVED: " + id);
        lock.lock();
        chunkMap.put(id, data);
        lock.unlock();
        chunkIdList.remove(id);
        notifyIfComplete();
    }

    // generate file
    public boolean generateFile(String filepath, List<ByteString> chunkIds) {
        synchronized (chunkIdList) {
            try {
                logger.log(Level.INFO, "Waiting for completion...");
                chunkIdList.wait();
                logger.log(Level.INFO, "-- GENERATING FILE...");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            List<String> keys = convertChunkIdList(chunkIds);
            lock.lock();
            Path path = Paths.get(filepath);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            logger.log(Level.INFO, "-- GATHERING DATA...");
            int i = 0;
            for (String key : keys) {
                // get data
                byte[] data = chunkMap.get(key);
                // write to file
                outputStream.write(data);
                logger.log(Level.INFO, "-- " + (++i) + " / " + chunkIds.size() + " COMPLETE");
            }
            lock.unlock();
            Files.write(path, outputStream.toByteArray());
            if(!new File(filepath).isFile()) {
                logger.log(Level.INFO, "-- ERROR WRITING TO FILE: " + filepath);
            }
            return true;
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
            return false;
        }
    }


    private void notifyIfComplete() {
        if(chunkIdList.size() == 0) {
            logger.log(Level.INFO, "-- ALL CHUNKS RECEIVED! ");
            synchronized (chunkIdList) {
                chunkIdList.notifyAll();
            }
        } else {
            logger.log(Level.INFO, "-- " + chunkIdList.size() + " CHUNKS WAITING");
        }
    }

    private CopyOnWriteArrayList<String> convertChunkIdList(List<ByteString> chunkIds) {
        CopyOnWriteArrayList<String> chunkIdList = new CopyOnWriteArrayList<>();
        for(ByteString id : chunkIds) {
            chunkIdList.add(DatatypeConverter.printHexBinary(id.toByteArray()));
        }
        return chunkIdList;
    }
    /** ------------------------------------------------------------------------------------- **/
}
