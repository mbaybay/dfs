package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created By: Melanie Baybay
 */

public class ControllerWorker implements Runnable {
    /**
     * ---------------------------------- DATA ATTRIBUTES ----------------------------------
     **/
    private final static Logger logger = Logger.getLogger(Controller.class.getName());
    private Socket requesterSock;
    private ControllerData data;
    /** ------------------------------------------------------------------------------------- **/

    /**
     * ------------------------------------ CONSTRUCTOR ------------------------------------
     **/
    public ControllerWorker(Socket sock, ControllerData data) {
        this.requesterSock = sock;
        this.data = data;
    }
    /** ------------------------------------------------------------------------------------- **/

    /**
     * ----------------------------------- PUBLIC METHODS ----------------------------------
     **/
    @Override
    public void run() {
        try {
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    this.requesterSock.getInputStream()
            );

            if (msgWrapper.hasAddStorageMsg()) {
                addStorageNode(msgWrapper.getAddStorageMsg());
            } else if (msgWrapper.hasStoreFileMsg()) {
                storeFile(msgWrapper.getStoreFileMsg());
            } else if (msgWrapper.hasRetrieveFileMsg()) {
                processFileRetrieval(msgWrapper.getRetrieveFileMsg());
            } else if (msgWrapper.hasHeartbeatMsg()) {
                processHeartbeat(msgWrapper.getHeartbeatMsg());
            } else if (msgWrapper.hasSendSummaryMsg()) {
                processSummaryRequest();
            } else if (msgWrapper.hasSendFileListMsg()) {
                processFileListRequest();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    /** ------------------------------------------------------------------------------------- **/
    /**
     * @param msg
     */
    private void addStorageNode(StorageMessages.StorageNode msg) {
        String hostname = msg.getHostname();
        int port = msg.getPort();
        this.data.addStorageNode(hostname, port);
        if (this.data.getStorageNodePort(hostname) != null) {
            logger.log(Level.FINE, "Added StorageNode - " + hostname + ":" + port);
        } else {
            logger.log(Level.SEVERE, "Error adding StorageNode - " + hostname + ":" + port);
        }
    }

    private void processHeartbeat(StorageMessages.Heartbeat msg) {
        // extract message info
        String hostname = msg.getStorageNode().getHostname();
        int port = msg.getStorageNode().getPort();
        long availStorage;
        long usedStorage;

        List<StorageMessages.ChunkMeta> chunkMetaList = msg.getChunkMetaList();

        if(msg.hasAvailDiskSpace() & msg.hasUsedDiskSpace()){
            availStorage = msg.getAvailDiskSpace();
            usedStorage = msg.getUsedDiskSpace();
            // include port in case it is the first heartbeat
            data.updateStorage(hostname, port, availStorage, usedStorage);
        }

        logger.log(Level.FINE, "HEARTBEAT: " + hostname + " " + port);

        if (chunkMetaList.size() > 0) {
            logger.log(Level.INFO, "-- " + chunkMetaList.size() + " CHUNK UPDATES --");
            for (StorageMessages.ChunkMeta chunkMeta : chunkMetaList) {
                // check chunk status for handling
                if(chunkMeta.getType() == StorageMessages.ChunkMeta.status.NEW) {
                    data.storedChunkUpdate(chunkMeta.getId().toByteArray());
                } else {
                    data.corruptChunkUpdate(chunkMeta.getId().toByteArray());
                }
            }
        }
        Timestamp ts = data.updateHeartbeatTime(hostname);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(new VerifyHeartbeat(hostname, ts), 8, TimeUnit.SECONDS);
        executor.shutdown();

    }

    private void processSummaryRequest() {
        logger.log(Level.INFO, "-- SUMMARY REQUEST");
        // extract info
        StorageMessages.StorageSummary.Builder builder = data.getStorageSummary();
        try {
            logger.log(Level.FINE, "- SENDING SUMMARY: " + requesterSock.getInetAddress().getHostName());
            StorageMessages.StorageMessageWrapper msgWapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setStorageSummaryMsg(builder)
                    .build();
            msgWapper.writeDelimitedTo(requesterSock.getOutputStream());
            logger.log(Level.INFO, "-- SUMMARY SENT");
        } catch (IOException e){
            logger.log(Level.SEVERE, e.toString());
        }
    }

    private void processFileListRequest() {
        logger.log(Level.INFO, "-- FILE LIST REQUEST");

        List<String> files = data.getFileList();
        logger.log(Level.INFO, "- RECEIVED FILE LIST");
        StorageMessages.FileList fileListMsg = StorageMessages.FileList.newBuilder()
                .addAllFilename(files)
                .build();

        try {
            logger.log(Level.FINE, "- SENDING FILE LIST");
            StorageMessages.StorageMessageWrapper msgWapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setFileListMsg(fileListMsg)
                    .build();
            msgWapper.writeDelimitedTo(requesterSock.getOutputStream());
            logger.log(Level.INFO, "- SENT FILE LIST");
        } catch (IOException e){
            logger.log(Level.SEVERE, e.toString());
        }
    }

    /**
     * @param storeFileMsg
     */
    private void storeFile(StorageMessages.StoreFile storeFileMsg) {

        final String filename = storeFileMsg.getFilename();
        logger.log(Level.INFO, " -- STORE FILE REQUEST: " + filename);
        // extract chunkIdList from protobuf message
        List<com.google.protobuf.ByteString> chunkIdList = storeFileMsg.getChunkIdList();

        // create chunkMeta data list
        ArrayList<String> chunkIds = new ArrayList<>();
        HashMap<String, List<String>> chunkStorageMap = new HashMap<>();
        logger.log(Level.FINE, " Generating " + chunkIdList.size() + "ChunkMeta Objects");
        for (ByteString id : chunkIdList) {
            String chunkId = DatatypeConverter.printHexBinary(id.toByteArray());
            chunkIds.add(chunkId);
            // nominate storage nodes
            chunkStorageMap.put(chunkId, nominateNodes());
        }
        data.addFile(filename, chunkIds, chunkStorageMap);

        logger.log(Level.FINE, "Added File to Controller Data");

        sendFileStorageList(chunkIds, chunkStorageMap);


        int nChunks = chunkIdList.size();
        int delay;
        if(nChunks < 5) {
            delay = 8 * chunkIdList.size();
        } else if (nChunks > 5 & nChunks < 20) {
            delay = 2 * chunkIdList.size();
        } else {
            delay = chunkIdList.size();
        }

        logger.log(Level.INFO, "Waiting for storage completion...");
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(new VerifyFileStorage(filename, requesterSock), delay, TimeUnit.SECONDS);
        executor.shutdown();
    }

    private void processFileRetrieval(StorageMessages.RetrieveFile retrieveFileMsg) {
        logger.log(Level.INFO, "-- FILE RETRIEVAL REQUEST");
        // extract filename
        String filename = retrieveFileMsg.getFileName();
        // GET LIST OF CHUNK IDS
        List<String> chunkIds = data.getFileChunkIds(filename);

        logger.log(Level.FINE, "Compiling chunk storage lists");
        // create FileStorageLists Message
        StorageMessages.FileStorageLists.Builder fslBuilder = StorageMessages.FileStorageLists.newBuilder();
        for(String id : chunkIds) {
            // create list of storageNode messages
            StorageMessages.ChunkStorage.Builder csBuilder = StorageMessages.ChunkStorage.newBuilder();
            // GET LIST OF STORAGE NODES for id
            List<String> chunkStorage = data.getChunkStorage(id);
            for(String storageNode : chunkStorage) {
                int port = data.getStorageNodePort(storageNode);
                csBuilder.addStorageNode(StorageMessages.StorageNode.newBuilder()
                    .setHostname(storageNode)
                    .setPort(port)
                );
            }

            // add chunkId and Storage to fileStorageList Builder
            fslBuilder.addChunkId(ByteString.copyFrom(DatatypeConverter.parseHexBinary(id)));
            fslBuilder.addChunkStorage(csBuilder);
        }
        logger.log(Level.INFO, "-- FILE STORAGE LIST COMPLETE");

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                .setFileStorageListsMsg(fslBuilder)
                .build();
        try {
            msgWrapper.writeDelimitedTo(this.requesterSock.getOutputStream());
            logger.log(Level.INFO, "-- SENT FILE STORAGE LIST: " + this.requesterSock.getInetAddress().getHostName());
            this.requesterSock.close();
        } catch(IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    /**
     * Randomly nominates storage nodes for a single chunk.
     *
     * @return: list of nominated storage nodes
     */
    private ArrayList<String> nominateNodes() {
        ArrayList<String> chunkStorageNodes = new ArrayList<>(data.nReplicas);
        for (int i = 0; i < data.nReplicas; i++) {
            int rand = ThreadLocalRandom.current().nextInt(0, data.getNumberOfStorageNodes());
            String storageHostname = data.getStorageNode(rand);

            // select new storage node if already selected for chunk
            // perform only if the number of available storage nodes > number of chunk replicas
            if ((chunkStorageNodes.contains(storageHostname)) && (data.getNumberOfStorageNodes() > data.nReplicas)) {
                while (chunkStorageNodes.contains(storageHostname)) {
                    rand = ThreadLocalRandom.current().nextInt(0, data.getNumberOfStorageNodes());
                    storageHostname = data.getStorageNode(rand);
                }
            }

            chunkStorageNodes.add(storageHostname);
        }
        logger.log(Level.FINE, "Nominated Nodes: " + chunkStorageNodes.toString());
        return chunkStorageNodes;
    }


    private void sendFileStorageList(ArrayList<String> chunkIds, Map<String, List<String>> chunkStorage) {
        logger.log(Level.FINE, "Sending Storage Node message...");
        StorageMessages.FileStorageLists.Builder fileStorageListsBuilder = StorageMessages.FileStorageLists.newBuilder();
        // iterate list of chunks for single file
        for (String id : chunkIds) {
            // build chunk storage list
            StorageMessages.ChunkStorage.Builder chunkStorageNodesBuilder
                    = StorageMessages.ChunkStorage.newBuilder();

            if(chunkStorage.containsKey(id)) {
                // iterate list of hostnames from chunkMeta
                for (String storageNode : chunkStorage.get(id)) {
                    // add StorageNode (as hostname and port) to chunkStorageNodesBuilder
                    chunkStorageNodesBuilder.addStorageNode(
                            StorageMessages.StorageNode.newBuilder()
                                    .setHostname(storageNode)
                                    .setPort(data.getStorageNodePort(storageNode))
                                    .build()
                    );
                }
                StorageMessages.ChunkStorage chunkStorageMsg = chunkStorageNodesBuilder.build();
                // add chunk storage to file storage list
                fileStorageListsBuilder.addChunkStorage(chunkStorageMsg);
            } else {
                logger.log(Level.SEVERE, "-- ISSUE WITH BUILDING CHUNK STORAGE LIST. Incorrect chunkId");
            }
        }

        // prep message to send to client
        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setFileStorageListsMsg(fileStorageListsBuilder)
                        .build();
        // send to client
        try {
            msgWrapper.writeDelimitedTo(requesterSock.getOutputStream());
            logger.log(Level.FINE, "Sent Storage Node Lists to Client");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public class VerifyFileStorage implements Runnable {
        /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
        private String filename;
        private Socket clientSock;
        /** ------------------------------------------------------------------------------------- **/

        /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
        public VerifyFileStorage(String filename, Socket cSock) {
            this.filename = filename;
            this.clientSock = cSock;
        }
        /** ------------------------------------------------------------------------------------- **/

        @Override
        public void run() {
            logger.log(Level.FINE, "Calling storage verification...");
            // check if chunks are still pending
            boolean complete = data.verifyFileStorageCompletion(filename);
            logger.log(Level.FINE, "Verification: " + complete);
            StorageMessages.StoreFileStatus.Builder fileStatusMsg;
            if (complete) {
                logger.log(Level.INFO, "-- SUCCESSFUL STORAGE: " + filename);
                fileStatusMsg = StorageMessages.StoreFileStatus.newBuilder()
                        .setFilename(filename)
                        .setType(StorageMessages.StoreFileStatus.status.SUCCESS);
            } else {
                logger.log(Level.WARNING, "Incomplete Storage: " + filename);
                fileStatusMsg = StorageMessages.StoreFileStatus.newBuilder()
                        .setFilename(filename)
                        .setType(StorageMessages.StoreFileStatus.status.FAIL);
            }
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setStoreFileStatusMsg(fileStatusMsg)
                    .build();
            try {
                // send status
                msgWrapper.writeDelimitedTo(clientSock.getOutputStream());
                clientSock.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
                System.exit(-1);
            }
        }
    }

    public class VerifyHeartbeat implements Runnable {

        private String hostname;
        private Timestamp ts;

        public VerifyHeartbeat(String hostname, Timestamp ts) {
            this.hostname = hostname;
            this.ts = ts;
        }

        @Override
        public void run() {
            boolean isDead = data.verifyHeartbeat(hostname, ts); // returns True if timestamps are the same...
            if(isDead) {
                logger.log(Level.SEVERE, "-- DEAD STORAGE NODE: " + hostname);
                // remove storage node from ContollerData
                List<String> replicaList = data.removeStorageNode(hostname);
                try {
                    for (String id : replicaList) {
                        // get contact info of host with chunk copy
                        List<String> hostList = data.getChunkStorage(id);
                        String host = hostList.get(hostList.size()-1);
                        Socket sock = new Socket(host, data.getStorageNodePort(host));

                        // nominate new node to replicate chunk
                        String newHost = data.nominateReplicaNode(id);
                        int port = data.getStorageNodePort(newHost);

                        // create storeReplicaMsg
                        StorageMessages.SendReplica storeReplicaMsg = StorageMessages.SendReplica.newBuilder()
                                .setChunkId(ByteString.copyFrom(DatatypeConverter.parseHexBinary(id)))
                                .setStorageNode(StorageMessages.StorageNode.newBuilder()
                                        .setHostname(newHost)
                                        .setPort(port))
                                .build();
                        StorageMessages.StorageMessageWrapper msgWrapper
                                = StorageMessages.StorageMessageWrapper.newBuilder()
                                .setSendReplicaMsg(storeReplicaMsg)
                                .build();
                        // send to old host
                        msgWrapper.writeDelimitedTo(sock.getOutputStream());
                        logger.log(Level.FINE, "Sending Replica request to: " + host);
                    }

                    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
                    executor.schedule(new VerifyReplication(replicaList), 10 * replicaList.size(), TimeUnit.SECONDS);
                    executor.shutdown();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, e.toString());
                }
            }
        }
    }

    public class VerifyReplication implements Runnable {
        List<String> chunkIds;

        public VerifyReplication(List<String> chunkIds) {
            this.chunkIds = chunkIds;
        }
        @Override
        public void run() {
            logger.log(Level.FINE, "Calling replication verification...");
            // check if chunks are still pending
            boolean complete = data.verifyChunkStorage(this.chunkIds);
            if (complete) {
                logger.log(Level.INFO, "-- REPLICATION SUCCESS");
            } else {
                logger.log(Level.SEVERE, "-- REPLICATION FAILED");

            }
        }
    }
}
