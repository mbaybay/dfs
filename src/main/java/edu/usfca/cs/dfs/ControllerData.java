package edu.usfca.cs.dfs;

import javax.xml.bind.DatatypeConverter;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created By: Melanie Baybay
 */
public class ControllerData {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private final static Logger logger = Logger.getLogger(Controller.class.getName());
    private ReentrantLock chunkLock;
    private ReentrantLock tempLock;
    private ReentrantLock pendingLock;
    private ReentrantLock storageLock;
    private ReentrantLock heartbeatLock;

    private HashMap<String, List<String>> fileMap; // filename, list of chunkIds
    private HashMap<String, List<String>> tempFileMap;
    private HashMap<String, List<String>> chunkMap; // chunkId, list of storageNodes
    private HashMap<String, List<String>> tempChunkMap;
    private HashMap<String, List<String>> storageChunksMap; // hostname, list of chunks

    private HashMap<String, Integer> pending;
    private HashMap<String, Timestamp> heartbeatTimes; // hostname, lastRecordedHeartbeat
    private HashMap<String, Integer> storageNodes; // hostname, port

    private CopyOnWriteArrayList<String> fileList;
    private CopyOnWriteArrayList<String> storageNames;
    private HashMap<String, Long> storageUsedMap; // hostname, bytes used
    private HashMap<String, Long> storageAvailMap; // hostname, bytes avail
    protected int nReplicas;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    public ControllerData(int nReplicas) {
        this.chunkLock = new ReentrantLock(true);
        this.tempLock = new ReentrantLock(true);
        this.pendingLock = new ReentrantLock(true);
        this.storageLock = new ReentrantLock(true);
        this.heartbeatLock = new ReentrantLock(true);
        this.fileMap = new HashMap<>();
        this.tempFileMap = new HashMap<>();
        this.chunkMap = new HashMap<>();
        this.tempChunkMap = new HashMap<>();
        this.pending = new HashMap<>();
        this.storageNodes = new HashMap<>();
        this.storageChunksMap = new HashMap<>();
        this.heartbeatTimes = new HashMap<>();
        this.fileList = new CopyOnWriteArrayList<>();
        this.storageNames = new CopyOnWriteArrayList<>();
        this.storageAvailMap = new HashMap<>();
        this.storageUsedMap = new HashMap<>();
        this.nReplicas = nReplicas;
    }
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public void addFile(String filename, List<String> chunkIds, Map<String, List<String>> storageNodes) {
        logger.log(Level.INFO, "-- ADD FILE");
        this.fileList.add(filename);

        // add file and chunkList to temp data structure
        this.tempLock.lock();
        this.tempFileMap.put(filename, chunkIds);
        this.tempChunkMap.putAll(storageNodes);
        this.tempLock.unlock();

        logger.log(Level.INFO, "-- ADD FILE");

        this.pendingLock.lock();
        for(String id : chunkIds) {
            this.pending.put(id, nReplicas);
        }
        this.pendingLock.unlock();

        logger.log(Level.INFO, "-- ADD FILE COMPLETE");
    }

    public void storedChunkUpdate(byte[] chunkId) {
        String id = DatatypeConverter.printHexBinary(chunkId);

        this.pendingLock.lock();
        logger.log(Level.INFO, "-- CHUNK STORED UPDATE: " + id);
        if (this.pending.containsKey(id)) {
            int nReplicas = this.pending.get(id) - 1; // update count
            logger.log(Level.INFO, "-- CHUNK REPLICAS: " + nReplicas);
            this.pending.remove(id);
            if (nReplicas > 0) {
                this.pending.put(id, nReplicas);
            }
        }
        logger.log(Level.INFO, "-- CHUNKS PENDING: " + this.pending.size());
        this.pendingLock.unlock();
        logger.log(Level.INFO, "-- CHUNK STORED UPDATE COMPLETE");
    }

    public void corruptChunkUpdate(byte[] chunkId) {
        logger.log(Level.SEVERE, "CORRUPT CHUNK UNHANDLED: " + DatatypeConverter.printHexBinary(chunkId));
    }

    public boolean verifyFileStorageCompletion(String filename) {
        logger.log(Level.INFO, "-- VERIFYING STORAGE COMPLETION -- ");
        // GET CHUNK IDS TO VERIFY
        this.tempLock.lock();
        List<String> chunkIds = this.tempFileMap.get(filename);
        this.tempLock.unlock();

        boolean result = verifyChunkStorage(chunkIds);

        if(result) {
            this.tempLock.lock();
            // MOVE filename->List<chunkIds> to FILEMAP
            this.fileMap.put(filename, this.tempFileMap.remove(filename));
            this.tempLock.unlock();
            logger.log(Level.INFO, "-- STORAGE COMPLETE: " + filename);
            return true;
        } else {
            logger.log(Level.INFO, "-- STORAGE INCOMPLETE: " + filename);
            return false;
        }
    }


    public boolean verifyChunkStorage(List<String> chunkIds) {
        logger.log(Level.INFO, chunkIds.size() + " CHUNKS TO VERIFY...");

        // CREATE PLACEHOLDERS TO MAP host->List<chunkIds> and chunkId->List<host>
        HashMap<String, List<String>> storageNodeChunks = new HashMap<>(); // host, List<chunkId>
        HashMap<String, List<String>> chunkStorageNodes = new HashMap<>(); // chunkId, List<host>
        this.pendingLock.lock();
        this.storageLock.lock();
        int i = 0;
        // ITERATE LIST OF CHUNK IDS TO VERIFY
        for (String id : chunkIds) {
            // VERIFY THAT CHUNK COMPLETED, EXIT IF INCOMPLETE
            if (this.pending.containsKey(id)) {
                logger.log(Level.SEVERE, i + "/" + chunkIds.size() + " verified");
                logger.log(Level.SEVERE, "Failed Chunk: " + id);;
                this.pendingLock.unlock();
                this.storageLock.unlock();
                return false;
            }
            // ITERATE LIST OF NOMINATED STORAGE NODES for this id
            for (String host : this.tempChunkMap.get(id)) {
                List<String> successIds;
                // GET CHUNK IDS THAT WERE SUCCESSFULLY STORED ON THIS HOST PREVIOUSLY
                if(this.storageChunksMap.containsKey(host)) {
                    successIds = this.storageChunksMap.get(host);
                    // OTHERWISE, CREATE NEW ARRAY LIST
                } else {
                    successIds = new ArrayList<>();
                }
                // ADD THIS id TO COPY OF SUCCESSFULLY STORED
                successIds.add(id);
                // ADD SUCCESSFUL CHUNK IDS TO host->List<chunkIds> PLACEHOLDER
                storageNodeChunks.put(host, successIds);
            }
            // MOVE LIST OF STORAGE NODES TO chunkId->List<host> PLACEHOLDER
            chunkStorageNodes.put(id, this.tempChunkMap.remove(id));

            logger.log(Level.INFO, ++i + "/" + chunkIds.size() + " verified");
        }

        // IF SUCCESSFUL: PLACEHOLDERS THAT MAP host->List<chunkIds> and chunkId->List<host>
        // ADD ALL host->List<chunkIds> to STORAGE CHUNKS
        this.storageChunksMap.putAll(storageNodeChunks);
        this.storageLock.unlock();
        this.pendingLock.unlock();

        // ADD ALL chunkId->List<host> to CHUNK MAP
        this.chunkLock.lock();
        this.chunkMap.putAll(chunkStorageNodes);
        this.chunkLock.unlock();

        return true;
    }


    public void addStorageNode(String hostname, int port) {
        this.storageLock.lock();
        this.storageNodes.put(hostname, port);
        this.storageNames.add(hostname);

        this.storageLock.unlock();

        this.heartbeatLock.lock();
        this.heartbeatTimes.put(hostname, new Timestamp(System.currentTimeMillis()));
        this.heartbeatLock.unlock();
    }

    public void updateStorage(String hostname, int port, long availSpace, long usedSpace) {
        this.storageLock.lock();
        if(this.storageNodes.get(hostname) == null) {
            this.storageLock.unlock();
            addStorageNode(hostname, port);
            this.storageLock.lock();
        }
        this.storageAvailMap.put(hostname, availSpace);
        this.storageUsedMap.put(hostname, usedSpace);
        this.storageLock.unlock();
    }

    public Timestamp updateHeartbeatTime(String hostname) {
        this.heartbeatLock.lock();
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        this.heartbeatTimes.put(hostname, ts);
        this.heartbeatLock.unlock();
        return ts;
    }

    public boolean verifyHeartbeat(String hostname, Timestamp ts) {
        this.heartbeatLock.lock();
        Timestamp oldTimestamp = this.heartbeatTimes.get(hostname);
        this.heartbeatLock.unlock();
        return oldTimestamp.equals(ts); // return True if timestamps are the same
    }

    public List<String> removeStorageNode(String hostname) {
        this.heartbeatLock.lock();
        this.heartbeatTimes.remove(hostname);
        this.heartbeatLock.unlock();

        // remove from storageNames, storageNodes, heartbeatTimes
        this.storageLock.lock();
        this.chunkLock.lock();
        List<String> chunkIds = this.storageChunksMap.get(hostname);
        this.storageChunksMap.remove(hostname);
        this.storageNames.remove(hostname);
        this.storageNodes.remove(hostname);

        // update chunkMap & pending
        for(String id : chunkIds) {
            List<String> storageNodes = this.chunkMap.get(id);
            storageNodes.remove(hostname);
            this.chunkMap.put(id, storageNodes);
        }
        this.chunkLock.unlock();
        this.storageLock.unlock();

        return chunkIds;
    }

    public String nominateReplicaNode(String chunkId) {
        this.chunkLock.lock();
        List<String> storageNodes = this.chunkMap.get(chunkId);
        this.chunkLock.unlock();
        logger.log(Level.INFO, "Nominating new storage for: " + chunkId);
        int rand = ThreadLocalRandom.current().nextInt(0, getNumberOfStorageNodes());
        while(storageNodes.contains(storageNames.get(rand))){
            rand = ThreadLocalRandom.current().nextInt(0, getNumberOfStorageNodes());
        }

        String host = storageNames.get(rand);
        storageNodes.add(host);
        logger.log(Level.INFO, "-- NOMINATED: " + host);

        // update pending & tempChunkMap
        this.tempLock.lock();
        this.pendingLock.lock();
        this.tempChunkMap.put(chunkId, storageNodes);
        this.pending.put(chunkId, 1);
        this.pendingLock.unlock();
        this.tempLock.unlock();

        return host;
    }


    public Integer getStorageNodePort(String hostname) {
        this.storageLock.lock();
        int port = this.storageNodes.get(hostname);
        this.storageLock.unlock();
        return port;
    }

    public List<String> getFileChunkIds(String filename){
        this.chunkLock.lock();
        List<String> chunkIds = this.fileMap.get(filename);
        this.chunkLock.unlock();
        return chunkIds;
    }

    public List<String> getChunkStorage(String chunkId) {
        this.chunkLock.lock();
        List<String> storageNodes = this.chunkMap.get(chunkId);
        this.chunkLock.unlock();
        return storageNodes;
    }

    public StorageMessages.StorageSummary.Builder getStorageSummary() {
        logger.log(Level.INFO, "- Preparing Storage Summary");
        float totalUsed = 0;
        float totalAvail = 0;

        this.storageLock.lock();
        logger.log(Level.INFO, "...adding avail and total space");
        for(String host : this.storageNames) {
            totalUsed += (float) this.storageUsedMap.get(host);
            totalAvail += (float) this.storageAvailMap.get(host);
        }
        logger.log(Level.INFO, ":: used: " + totalUsed + "\n:: avail: " + totalAvail);
        this.storageLock.unlock();

        // convert to GB
        totalUsed /= 1000000000;
        totalAvail /= 1000000000;

        logger.log(Level.INFO, "- TotalUsed and TotalAvail complete");

        StorageMessages.StorageSummary.Builder builder = StorageMessages.StorageSummary.newBuilder()
                .setTotalUsed(totalUsed)
                .setTotalAvail(totalAvail)
                .setNHosts(getNumberOfStorageNodes());
        return builder;
    }

    public List<String> getFileList() {
        return fileList;
    }

    public String getStorageNode(int i) {
        return this.storageNames.get(i);
    }

    public int getNumberOfStorageNodes() {
        return this.storageNames.size();
    }
    /** ------------------------------------------------------------------------------------- **/
}
