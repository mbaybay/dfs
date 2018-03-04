package edu.usfca.cs.dfs;


import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class StorageNode {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private final static Logger logger = Logger.getLogger(StorageNode.class.getName());
    private final static String BASEDIR = "/home2/mbaybay/";
    private final int DEFAULT_HEARTBEAT_THREADS = 5;

    private ServerSocket sock;
    private String controllerHostname;
    private int controllerPort;
    private long usedDiskSpace;
    private StorageMessages.Heartbeat.Builder heartbeat;
    private StorageMessages.StorageNode storageNodeMsg;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    public StorageNode(String cHostname, int cPort) {
        this.sock = null;
        this.controllerHostname = cHostname;
        this.controllerPort = cPort;
        this.usedDiskSpace = 0;
    }
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public static void main(String[] args) 
    throws Exception {
        // add file handler for storing logs
        try {
            File dir = new File(BASEDIR + "logs/");
            if (!dir.isDirectory()) {
                dir.mkdir();
            }
            FileHandler fh = new FileHandler(dir.getPath() + "/StorageNode.log");
            logger.addHandler(fh);
            logger.log(Level.INFO, "FileHandler: " + dir.getPath() + "/StorageNode.log");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Unable set StorageNode log file: " + e.toString() + "\nProceeding...");
        }
        logger.setLevel(Level.INFO);
        new StorageNode(args[0], Integer.parseInt(args[1])).start();
    }

    /**
     * At startup: send addStorage message to controller
     */
    private void start() {
        try {
            // Create checksum dir, if not present
            File dir = new File(BASEDIR + "checksums/");
            if (!dir.isDirectory()) {
                dir.mkdir();
            }
            // STARTUP SERVER
            int port = new PortManager().getAvailablePort();
            if (port == -1) {
                System.err.println("No available port in range 14000-14999");
                System.exit(-1);
            }
            this.sock = new ServerSocket(port);
            logger.log(Level.INFO, "StorageNode Created on " + getHostname() + ":" + getPort() +  "...");


            // init heartbeat
            initHeartbeat();

            // spawn thread for handling storage requests
            (new Thread(new StorageHandler())).start();

            // send heartbeats every 5 seconds
            ScheduledExecutorService heartbeatMaster = Executors.newScheduledThreadPool(DEFAULT_HEARTBEAT_THREADS);
            heartbeatMaster.scheduleAtFixedRate(
                    new HeartbeatWorker(),
                    0L,
                    5L,
                    TimeUnit.SECONDS);

        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PRIVATE METHODS --------------------------------- **/
    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }


    private int getPort() {
        if(this.sock != null) {
            return this.sock.getLocalPort();
        } else {
            logger.log(Level.SEVERE, "Port requested but has not been set.");
            return -1;
        }
    }

    private void initHeartbeat() {
        try {
            this.heartbeat = StorageMessages.Heartbeat.newBuilder()
                    .setStorageNode(StorageMessages.StorageNode.newBuilder()
                            .setHostname(getHostname())
                            .setPort(getPort())
                            .build())
                    .setAvailDiskSpace(getAvailableDiskSpace())
                    .setUsedDiskSpace(this.usedDiskSpace);
            logger.log(Level.FINE, "init first heartbeat");
        } catch (UnknownHostException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    private void newChunkHeartbeatUpdate(byte[] chunkId, long size) {
        this.heartbeat.addChunkMeta(StorageMessages.ChunkMeta.newBuilder()
                        .setId(ByteString.copyFrom(chunkId))
                        .setType(StorageMessages.ChunkMeta.status.NEW)
        );
        this.heartbeat.setAvailDiskSpace(getAvailableDiskSpace());
        this.heartbeat.setUsedDiskSpace(this.usedDiskSpace + size);
        logger.log(Level.FINE, "New Chunk: size=" + this.heartbeat.getUsedDiskSpace());
        this.usedDiskSpace += size;
    }

    private void corruptChunkHeartbeatUpdate(byte[] chunkId) {
        this.heartbeat.addChunkMeta(StorageMessages.ChunkMeta.newBuilder()
                        .setId(ByteString.copyFrom(chunkId))
                        .setType(StorageMessages.ChunkMeta.status.CORRUPT)
        );
        logger.log(Level.INFO, "Corrupt Chunk: " + DatatypeConverter.printHexBinary(chunkId));
    }

    private long getAvailableDiskSpace() {
        return new File(this.BASEDIR).getUsableSpace();
    }


    public class StorageHandler implements Runnable{
        /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
        @Override
        public void run() {
            try {
                while (true) {
                    logger.log(Level.INFO, "-- LISTENING ON: " + getHostname() + ":" + getPort() +  "...");
                    Socket socket = sock.accept();
                    if (socket.isConnected()) {
                        handleRequest(socket);
                    }
                }
            } catch (IOException e) {
                logger.log(Level.INFO, e.toString());
            }
        }
        /** ------------------------------------------------------------------------------------- **/

        /** ----------------------------------- PRIVATE METHODS --------------------------------- **/

        private void handleRequest(Socket sock) throws IOException {
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    sock.getInputStream()
            );

            if (msgWrapper.hasStoreChunkMsg()) {
                storeChunk(msgWrapper.getStoreChunkMsg());
            } else if (msgWrapper.hasRetrieveChunkMsg()) {
                retrieveChunk(sock, msgWrapper.getRetrieveChunkMsg());
            } else if (msgWrapper.hasSendReplicaMsg()) {
                sendReplica(msgWrapper.getSendReplicaMsg());
            } else {
                logger.log(Level.SEVERE, "-- UNKNOWN message: " + msgWrapper.getMsgCase().name());
            }
        }

        private void storeChunk(StorageMessages.StoreChunk storeChunkMsg) {
            logger.log(Level.INFO, "--- STORE CHUNK");
            // extract chunkId and checksum
            byte[] chunkId = storeChunkMsg.getMeta().getId().toByteArray();
            byte[] checksum = storeChunkMsg.getMeta().getChecksum().toByteArray();
            byte[] data = storeChunkMsg.getData().toByteArray();

            logger.log(Level.FINE, "Received chunk " + DatatypeConverter.printHexBinary(chunkId));
            logger.log(Level.FINE, "Received checksum " + DatatypeConverter.printHexBinary(checksum));

            // store chunk data as binary file
            try {
                // verify checksum
                if(compareChecksum(data, checksum)) {
                    // store data in BASEDIR/chunkID
                    Path dataPath = Paths.get(BASEDIR, DatatypeConverter.printHexBinary(chunkId));
                    Files.write(dataPath, data);
                    // store checksum in BASEDIR/checksums/chunkID
                    Path chksumPath = Paths.get(BASEDIR + "checksums/", DatatypeConverter.printHexBinary(chunkId));
                    Files.write(chksumPath, checksum);

                    logger.log(Level.INFO, "Successful chunk store. Adding Update to heartbeat.");

                    // add change to heartbeat
                    newChunkHeartbeatUpdate(chunkId, data.length);

                    // send replica, if needed
                    if(storeChunkMsg.hasChunkStorage()) {
                        if(storeChunkMsg.getChunkStorage().getStorageNodeList().size() > 0) {
                            sendStoreReplicaMsg(storeChunkMsg);
                        } else {
                            logger.log(Level.FINE, "Replica Storage Complete.");
                        }
                    }
                } else {
                    logger.log(Level.SEVERE, "CORRUPT DATA/CHECKSUM. Updating heartbeat.");
                    corruptChunkHeartbeatUpdate(chunkId);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        private void sendReplica(StorageMessages.SendReplica storeReplicaMsg){
            logger.log(Level.INFO, "-- SEND REPLICA");
            try {
                // extract msg content
                ByteString chunkId = storeReplicaMsg.getChunkId();
                String id = DatatypeConverter.printHexBinary(chunkId.toByteArray());
                String host = storeReplicaMsg.getStorageNode().getHostname();
                int port = storeReplicaMsg.getStorageNode().getPort();
                // check if file exists...
                File chunkFile = new File(BASEDIR + id);
                if(chunkFile.isFile()){
                    Path filepath = Paths.get(BASEDIR + id);
                    Path checksumPath = Paths.get(BASEDIR + "checksums/", id);
                    // get file data into byte array
                    byte[] data = Files.readAllBytes(filepath);
                    // checksum file before sending
                    byte[] checksum = Files.readAllBytes(checksumPath);
                    if(compareChecksum(data, checksum)) {
                        logger.log(Level.FINE, "Preparing StoreChunk message...");
                        Socket sock = new Socket(host, port);

                        StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder()
                                .setMeta(StorageMessages.ChunkMeta.newBuilder()
                                        .setId(chunkId)
                                        .setChecksum(ByteString.copyFrom(checksum))
                                        .setType(StorageMessages.ChunkMeta.status.NEW))
                                .setData(ByteString.copyFrom(data))
                                .build();
                        StorageMessages.StorageMessageWrapper msgWrapper
                                = StorageMessages.StorageMessageWrapper.newBuilder()
                                    .setStoreChunkMsg(storeChunkMsg)
                                    .build();
                        msgWrapper.writeDelimitedTo(sock.getOutputStream());
                        logger.log(Level.INFO, "-- SENDING REPLICA: " + host);
                    } else {
                        logger.log(Level.SEVERE, "INVALID DATA: Does not match checksum.");
                    }
                } else {
                    logger.log(Level.SEVERE, "ERROR: FILE DOES NOT EXIST: " + id);
                }

            } catch (IOException e) {
                logger.log(Level.INFO, e.toString());
            }
        }


        private void sendStoreReplicaMsg(StorageMessages.StoreChunk storeChunkMsg) {
            logger.log(Level.FINE, "Preparing replica storage for chunk "
                    + DatatypeConverter.printHexBinary(storeChunkMsg.getMeta().getId().toByteArray()));
            try {
                // extract list of storage nodes for chunk
                if(storeChunkMsg.hasChunkStorage()) {
                    List<StorageMessages.StorageNode> storageNodes =
                            storeChunkMsg.getChunkStorage().getStorageNodeList();
                    logger.log(Level.FINE, "Connecting to " + storageNodes.get(0).getHostname()
                            + " " + storageNodes.get(0).getPort() + "...");
                    // create connection to storage node
                    Socket storageNodeSock = new Socket(storageNodes.get(0).getHostname(), storageNodes.get(0).getPort());
                    logger.log(Level.FINE, "Connected");

                    logger.log(Level.FINE, "Connected");
                    logger.log(Level.FINE, "Building storeReplicaMsg...");
                    // build message to store replica,
                    StorageMessages.StoreChunk storeReplicaMsg = StorageMessages.StoreChunk.newBuilder()
                            .setData(storeChunkMsg.getData())
                            .setMeta(storeChunkMsg.getMeta())
                            .setChunkStorage(
                                    // chunkStorage[1:] since we send to chunkStorage[0]
                                    StorageMessages.ChunkStorage.newBuilder()
                                            .addAllStorageNode(
                                                    storageNodes.subList(1, storageNodes.size())
                                            )
                                            .build()
                            )
                            .build();
                    logger.log(Level.FINE, "Wrapping storeReplicaMsg...");
                    // prep message
                    StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                            .setStoreChunkMsg(storeReplicaMsg)
                            .build();
                    logger.log(Level.FINE, "Sending...");
                    // send to storageNode
                    msgWrapper.writeDelimitedTo(storageNodeSock.getOutputStream());
                    logger.log(Level.FINE, "Sent replica to " + storageNodes.get(0).getHostname()
                            + " " + storageNodes.get(0).getPort());
                    storageNodeSock.close();
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        private void retrieveChunk(Socket sock, StorageMessages.RetrieveChunk retrieveChunkMsg) {
            // extract msg info
            String id = DatatypeConverter.printHexBinary(retrieveChunkMsg.getChunkId().toByteArray());
            // prepare msg to client
            StorageMessages.RetrieveChunk.Builder rcBuilder = StorageMessages.RetrieveChunk.newBuilder()
                    .setChunkId(retrieveChunkMsg.getChunkId());
            Path filepath = Paths.get(BASEDIR, id);
            Path checksumPath = Paths.get(BASEDIR + "checksums/", id);
            try {
                logger.log(Level.INFO, "Reading Data & Checksum of CHUNK ID: " + id);
                // extract data from file
                byte[] data = Files.readAllBytes(filepath);
                // compare to checksum
                byte[] checksum = Files.readAllBytes(checksumPath);
                if(compareChecksum(data, checksum)){
                    logger.log(Level.FINE, "DATA VALID. Preparing to send...");
                    rcBuilder.setData(ByteString.copyFrom(data));
                    rcBuilder.setType(StorageMessages.RetrieveChunk.status.VALID);
                } else {
                    logger.log(Level.FINE, "DATA INVALID. Preparing to notify client...");
                    rcBuilder.setType(StorageMessages.RetrieveChunk.status.CORRUPT);
                }
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                        .setRetrieveChunkMsg(rcBuilder)
                        .build();
                msgWrapper.writeDelimitedTo(sock.getOutputStream());
                sock.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        private boolean compareChecksum(byte[] data, byte[] checksum) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(data);
                byte[] dataChecksum = md.digest();
                if(Arrays.equals(dataChecksum, checksum)) {
                    return true;
                } else {
                    return false;
                }
            } catch (NoSuchAlgorithmException e) {
                logger.log(Level.SEVERE, e.toString());
                return false;
            }
        }
        /** ------------------------------------------------------------------------------------- **/
    }

    /** ------------------------------------------------------------------------------------- **/

    public class HeartbeatWorker implements  Runnable{
        /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
        @Override
        public void run() {
            // Scheduled Execution of Heartbeats
            try{
                Socket controllerSock = new Socket(controllerHostname, controllerPort);
                // Wrap Message
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                        .setHeartbeatMsg(heartbeat)
                        .build();
                // send to controller
                logger.log(Level.FINE, "Sending heartbeat...");
                msgWrapper.writeDelimitedTo(controllerSock.getOutputStream());
                logger.log(Level.INFO, "SENT HEARTBEAT.");
                controllerSock.close();
                // clear chunk updates in heartbeat
                heartbeat.clearChunkMeta();
                heartbeat.clearAvailDiskSpace();
                heartbeat.clearAvailDiskSpace();
            } catch(IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }
        /** ------------------------------------------------------------------------------------- **/
    }


}
