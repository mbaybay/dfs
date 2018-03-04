package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.DatatypeConverter;

public class Client {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private final static Logger logger = Logger.getLogger(Client.class.getName());
    private static FileHandler fh = null;
    private final static int MAX_CHUNK_SIZE = 512000; // 512kb = 0.5 MB
    private final static String BASEDIR = "/home2/mbaybay/";
    protected String id;
    protected ServerSocket sock;
    private ClientRetrievalData retrievalData;
    protected WorkQueue taskQueue;
    private String controllerHostname;
    private int controllerPort;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    public Client(String cHostname, int cPort) {
        this.id = generateId();
        this.taskQueue = new WorkQueue();
        this.controllerHostname = cHostname;
        this.controllerPort = cPort;
    }
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public static void main(String[] args) {
        // initializing LOGGER
        try {
            File dir = new File(BASEDIR + "logs/");
            if (!dir.isDirectory()) {
                dir.mkdir();
            }
            fh = new FileHandler(dir.getPath() + "Client.log");
            logger.addHandler(fh);
            logger.log(Level.INFO, "FileHandler: " + dir.getPath() + "Client.log");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Unable set Client log file: " + e.toString());
        }
        logger.setLevel(Level.INFO);
    }

    public void start() {
        try {
            // select random port in range 14000 -14999
            int port = new PortManager().getAvailablePort();
            if (port != -1) {
                this.sock = new ServerSocket(port);
                logger.log(Level.INFO, "Starting Client on " + getHostname() + " " + getPort());
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
            System.exit(-1);
        }
    }

    public void storeFile(String filepath) throws Exception{
        // chunk file
        ArrayList<byte[]> dataChunks = chunkFile(filepath);
        // create unique ID for each chunk
        ArrayList<byte[]> chunkIdList = generateChunkIds(filepath, dataChunks.size());

        ArrayList<byte[]> checksumList = generateChecksumList(dataChunks);

        // extract filename from path
        String filename = extractFilename(filepath);

        // SEND STORAGE REQUEST TO CONTROLLER
        // building StoreFile protobuf
        StorageMessages.StoreFile.Builder storeFileBuilder = StorageMessages.StoreFile.newBuilder()
                        .setClientName(getHostname())
                        .setClientPort(getPort())
                        .setFilename(filename);

        // add list of chunkIds to protobuf
        for(byte[] chunkId : chunkIdList) {
            storeFileBuilder.addChunkId(ByteString.copyFrom(chunkId));
        }

        StorageMessages.StoreFile storeFileMsg = storeFileBuilder.build();

        // compile and wrap message
        StorageMessages.StorageMessageWrapper sendMsgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreFileMsg((storeFileMsg))
                        .build();
        logger.log(Level.FINE, "Created store file message.");

        // send message
        Socket socket = new Socket(controllerHostname, controllerPort);
        sendMsgWrapper.writeDelimitedTo(socket.getOutputStream());
        logger.log(Level.INFO, "-- SENT STORE FILE REQUEST");
        logger.log(Level.FINE, "Waiting for Controller response...");

        while (socket.isConnected()) {
            // wait for fileStorageList message
            StorageMessages.StorageMessageWrapper rcvdMsgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            if (rcvdMsgWrapper.hasFileStorageListsMsg()) {
                logger.log(Level.FINE, "Received FileStorageLists...");
                // unpack chunkStorageLists from FileStorageList
                List<StorageMessages.ChunkStorage> chunkStorageList = rcvdMsgWrapper
                        .getFileStorageListsMsg()
                        .getChunkStorageList();
                if(chunkStorageList != null) {
                    // send chunk to first storageNode of each chunkStorage
                    for(int i=0; i < chunkStorageList.size(); i++) {
                        // extract first storage node to send first replica
                        StorageMessages.StorageNode storageNode =
                                chunkStorageList.get(i).getStorageNode(0);
                        logger.log(Level.INFO, "-- SENDING CHUNK " + i + " to: " + storageNode.getHostname());
                        // create socket for storage node
                        Socket storageSock = new Socket(storageNode.getHostname(), storageNode.getPort());
                        // add sendChunk to task list
                        taskQueue.execute(new SendChunk(storageSock, dataChunks.get(i), chunkIdList.get(i),
                                        checksumList.get(i), chunkStorageList.get(i)));
                    }
                }
            } else if (rcvdMsgWrapper.hasStoreFileStatusMsg()) {
                logger.log(Level.INFO, "Received StoreFileStatus...");
                // extract msg
                StorageMessages.StoreFileStatus statusMsg = rcvdMsgWrapper.getStoreFileStatusMsg();
                String fname = statusMsg.getFilename();
                StorageMessages.StoreFileStatus.status status = statusMsg.getType();
                if(status == StorageMessages.StoreFileStatus.status.SUCCESS) {
                    logger.log(Level.INFO, "-- SUCCESSFUL FILE STORE: " + fname);
                } else {
                    logger.log(Level.INFO, "-- FAILED FILE STORE: " + fname);
                }
                socket.close();
                logger.log(Level.FINE, "Closed Connection to Controller");
                break;
            }
        }
        if(socket.isConnected()) {
            socket.close();
        }
    }


    public void retrieveFile(String filepath) {
        // request chunk storage node lists from controller
        try {
            String filename = extractFilename(filepath);
            // connect to controller
            Socket socket = new Socket(controllerHostname, controllerPort);
            // building RequestFile protobuf
            StorageMessages.RetrieveFile.Builder retrieveFileBuilder = StorageMessages.RetrieveFile.newBuilder()
                    .setFileName(filename);
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setRetrieveFileMsg(retrieveFileBuilder)
                    .build();
            msgWrapper.writeDelimitedTo(socket.getOutputStream());

            // get storage node lists - will close sock
            StorageMessages.FileStorageLists fileStorageLists = getFileStorageLists(socket);
            if(fileStorageLists != null) {
                // extract in-order list of chunkIds
                List<ByteString> chunkIds = fileStorageLists.getChunkIdList();
                // init retrievalData
                this.retrievalData = new ClientRetrievalData(chunkIds);
                // extract in-order list of chunkStorage
                List<StorageMessages.ChunkStorage> chunkStorageLists = fileStorageLists.getChunkStorageList();
                // init retrieval task for each chunk
                for(int i=0; i < chunkStorageLists.size(); i++) {
                    taskQueue.execute(new RetrieveChunk(chunkIds.get(i), chunkStorageLists.get(i)));
                }
                generateFile(filename, chunkIds);
            } else {
                logger.log(Level.SEVERE, "ChunkStorageList returned NULL");
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    public void getFileList() {
        try {
            logger.log(Level.FINE, "Requesting File List...");
            Socket cSock = new Socket(controllerHostname, controllerPort);
            StorageMessages.SendFileList.Builder builder = StorageMessages.SendFileList.newBuilder()
                    .setHostname(getHostname())
                    .setPort(getPort());
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setSendFileListMsg(builder)
                    .build();
            msgWrapper.writeDelimitedTo(cSock.getOutputStream());
            logger.log(Level.INFO, "REQUEST SENT... ");

            while (cSock.isConnected()) {
                StorageMessages.StorageMessageWrapper rcvdMsgWrapper;
                // wait for fileStorageList message
                rcvdMsgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        cSock.getInputStream());
                if (rcvdMsgWrapper.hasFileListMsg()) {
                    logger.log(Level.FINE, "Received FileLists...");
                    sock.close();
                    // unpack
                    List<String> files = rcvdMsgWrapper.getFileListMsg().getFilenameList();
                    displayFileList(files);
                    cSock.close();
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    public void getStorageSummary() {
        try {
            logger.log(Level.INFO, "Requesting Storage Summary...");
            Socket cSock = new Socket(controllerHostname, controllerPort);
            StorageMessages.SendSummary.Builder builder = StorageMessages.SendSummary.newBuilder()
                    .setHostname(getHostname())
                    .setPort(getPort());
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setSendSummaryMsg(builder)
                    .build();
            msgWrapper.writeDelimitedTo(cSock.getOutputStream());

            StorageMessages.StorageSummary storageSummaryMsg = getStorageSummary(cSock);
            DecimalFormat ft = new DecimalFormat("#.##");
            // unpack message
            double totalAvail = storageSummaryMsg.getTotalAvail();
            double totalUsed = storageSummaryMsg.getTotalUsed();

            System.out.println("\n\n-----------------------------");
            System.out.println("Total Available Disk Space: " + ft.format(totalAvail) + " GB");
            System.out.println("Total Used Disk Space:   " + ft.format(totalUsed) + " GB");
            System.out.println("-----------------------------\n\n");

        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    public void shutdown() {
        try {
            logger.log(Level.FINE, "Closing TaskQueue");
            taskQueue.close();
            logger.log(Level.FINE, "Shutting down TaskQueue");
            taskQueue.shutdown();
            logger.log(Level.FINE, "Shutting down socket");
            sock.close();
            logger.log(Level.INFO, "Shutdown Complete.");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error when closing socket.");
        }
    }

    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PRIVATE METHODS ---------------------------------- **/
    private String generateId() {
        logger.log(Level.INFO, "Generating ClientID");
        String id = UUID.randomUUID().toString();
        System.out.println("Client ID: " + id);
        return id;
    }

    private ArrayList<byte[]> generateChunkIds(String filename, int nChunks) {
        logger.log(Level.INFO, "Generating ChunkIDs");
        ArrayList<byte[]> chunkIds = new ArrayList<>();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (int i = 0; i < nChunks; i++) {
                md.update((filename + i).getBytes());
                chunkIds.add(md.digest());
                md.reset();
            }
        } catch (NoSuchAlgorithmException e) {
            logger.log(Level.SEVERE, e.toString());
        }

        return chunkIds;
    }

    private ArrayList<byte[]> generateChecksumList(ArrayList<byte[]> chunks) {
        logger.log(Level.INFO, "Generating Checksums");

        ArrayList<byte[]> checksumList = new ArrayList<>();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (int i = 0; i < chunks.size(); i++) {
                md.update(chunks.get(i));
                checksumList.add(md.digest());
                md.reset();
            }
        } catch (NoSuchAlgorithmException e) {
            logger.log(Level.SEVERE, e.toString());
        }
        return checksumList;
    }

    private ArrayList<byte[]> chunkFile(String filename) {
        logger.log(Level.INFO, "Chunking file: " + filename);

        byte[] chunk;
        ArrayList<byte[]> chunks = new ArrayList<>();
        // read file from input stream
        try {
            FileInputStream in = new FileInputStream(new File(filename));
            while (in.available() > 0) {
                if (in.available() < MAX_CHUNK_SIZE) {
                    chunk = new byte[in.available()];
                } else {
                    chunk = new byte[MAX_CHUNK_SIZE];
                }
                in.read(chunk);
                chunks.add(chunk);
            }
        } catch (FileNotFoundException e) {
            logger.log(Level.SEVERE, e.toString());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
        return chunks;
    }

    private String extractFilename(String filepath) {
        Pattern p = Pattern.compile("(?:[\\W\\w\\d]*/)([^/]*)");
        Matcher m = p.matcher(filepath);
        if(m.find()){
            return m.group(1);
        } else {
            return filepath;
        }
    }

    private StorageMessages.FileStorageLists getFileStorageLists(Socket sock) {
        try {
            while (sock.isConnected()) {
                StorageMessages.StorageMessageWrapper rcvdMsgWrapper;
                // wait for fileStorageList message
                rcvdMsgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        sock.getInputStream());
                if (rcvdMsgWrapper.hasFileStorageListsMsg()) {
                    logger.log(Level.INFO, "Received FileStorageLists...");
                    sock.close();
                    // unpack chunkStorageLists from FileStorageList
                    return rcvdMsgWrapper.getFileStorageListsMsg();
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
        return null;
    }

    private StorageMessages.StorageSummary getStorageSummary(Socket sock) {
        try {
            while (sock.isConnected()) {
                StorageMessages.StorageMessageWrapper rcvdMsgWrapper;
                // wait for fileStorageList message
                rcvdMsgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        sock.getInputStream());
                if (rcvdMsgWrapper.hasStorageSummaryMsg()) {
                    logger.log(Level.INFO, "Received StorageSummary...");
                    sock.close();
                    // unpack chunkStorageLists from FileStorageList
                    return rcvdMsgWrapper.getStorageSummaryMsg();
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
        return null;
    }

    private void generateFile(String filename, List<ByteString> chunkIds) {
        logger.log(Level.INFO, "-- RETRIEVAL INITIALIZED: " + filename);
        String newFilePath = BASEDIR + filename;
        logger.log(Level.FINE, "-- OUTPUT PATH: " + newFilePath);
        boolean status = retrievalData.generateFile(newFilePath, chunkIds);
        if(status) {
            logger.log(Level.INFO, "-- SUCCESSFUL RETRIEVAL: " + newFilePath);
        } else {
            logger.log(Level.SEVERE, "-- FAILED RETRIEVAL: " + newFilePath);
        }
    }

    private void displayFileList(List<String> files) {
        System.out.println("\n\n-----------------------------");
        System.out.println("-- FILE LIST --");
        for(String file : files) {
            System.out.println(file);
        }
        System.out.println("-----------------------------\n\n");
    }
    /** ------------------------------------------------------------------------------------- **/

    private void printChunkSizes(ArrayList<byte[]> chunks) {
        float total = 0;
        for(int i = 0; i < chunks.size(); i++) {
            float bytes = chunks.get(i).length;
            if(bytes < 1000) {
                System.out.println("\tchunk " + i + ": " + bytes + " bytes");
            } else {
                float kbSize =  bytes / 1000;
                total += kbSize;
                System.out.println("\tchunk " + i + ": " + kbSize + " kb");
            }
        }
        System.out.println("total: " + total + " kb");
    }

    private void printList(String title, ArrayList<byte[]> list) {
        for (int i = 0; i < list.size(); i++) {
            System.out.println(title + " " + i + ": " +
                    DatatypeConverter.printHexBinary(list.get(i)));
        }
        System.out.println();
    }

    private String getHostname(){
        return this.sock.getInetAddress().getHostName();
    }

    private int getPort(){
        return this.sock.getLocalPort();
    }

    public class SendChunk implements Runnable{
        /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
        private Socket sock;
        private byte[] chunkId;
        private byte[] checksum;
        private byte[] data;
        private List<StorageMessages.StorageNode> storageNodes;
        /** ------------------------------------------------------------------------------------- **/

        /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
        public SendChunk(Socket sock, byte[] data, byte[] chunkId, byte[] checksum,
                            StorageMessages.ChunkStorage chunkStorageMsg) {
            this.sock = sock;
            this.chunkId = chunkId;
            this.checksum = checksum;
            this.data = data;
            this.storageNodes = chunkStorageMsg.getStorageNodeList();
        }
        /** ------------------------------------------------------------------------------------- **/

        /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
        public void run() {
            try {
                logger.log(Level.FINE, "Building storeChunkMsg for chunk " + this.chunkId + "...");
                // create storeChunk message
                StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder()
                        .setData(ByteString.copyFrom(this.data))
                        .setMeta(
                                StorageMessages.ChunkMeta.newBuilder()
                                        .setId(ByteString.copyFrom(this.chunkId))
                                        .setChecksum(ByteString.copyFrom(this.checksum))
                        )
                        .setChunkStorage(
                                StorageMessages.ChunkStorage.newBuilder()
                                        .addAllStorageNode(storageNodes.subList(1, storageNodes.size()))
                        )
                        .build();
                logger.log(Level.FINE, "Wrapping storeChunkMsg...");
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreChunkMsg(storeChunkMsg)
                        .build();
                msgWrapper.writeDelimitedTo(sock.getOutputStream());
                logger.log(Level.FINE, "SENT CHUNK " + " to "
                        + sock.getInetAddress().getHostName() + " " + sock.getPort());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public class RetrieveChunk implements Runnable {
        /**
         * ---------------------------------- DATA ATTRIBUTES ----------------------------------
         **/
        private ByteString chunkId;
        private List<StorageMessages.StorageNode> storageNodes;
        /** ------------------------------------------------------------------------------------- **/

        /**
         * ------------------------------------ CONSTRUCTOR ------------------------------------
         **/
        public RetrieveChunk(ByteString chunkId, StorageMessages.ChunkStorage chunkStorage) {
            this.chunkId = chunkId;
            this.storageNodes = chunkStorage.getStorageNodeList();
        }
        /** ------------------------------------------------------------------------------------- **/

        /**
         * ----------------------------------- PUBLIC METHODS ----------------------------------
         **/
        public void run() {
            try {
                logger.log(Level.INFO, "RETRIEVING: "
                        + DatatypeConverter.printHexBinary(this.chunkId.toByteArray()));
                // loop storageNodes
                for (StorageMessages.StorageNode sn : storageNodes) {
                    // create socket
                    Socket storageSock = new Socket(sn.getHostname(), sn.getPort());
                    // send request to storageNode
                    StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                            .setRetrieveChunkMsg(
                                    StorageMessages.RetrieveChunk.newBuilder()
                                            .setChunkId(this.chunkId)
                                            .build()
                            )
                            .build();
                    msgWrapper.writeDelimitedTo(storageSock.getOutputStream());

                    StorageMessages.StorageMessageWrapper msgWrapaper
                            = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(storageSock.getInputStream());
                    if (msgWrapaper.hasRetrieveChunkMsg()) {
                        StorageMessages.RetrieveChunk retrieveChunkMsg = msgWrapaper.getRetrieveChunkMsg();
                        String id = DatatypeConverter.printHexBinary(retrieveChunkMsg.getChunkId().toByteArray());
                        StorageMessages.RetrieveChunk.status status = retrieveChunkMsg.getType();
                        if (status == StorageMessages.RetrieveChunk.status.VALID) {
                            byte[] data = retrieveChunkMsg.getData().toByteArray();
                            retrievalData.put(id, data);
                            logger.log(Level.INFO, "CHUNK RETRIEVED: " + id);
                            break;
                        } else if (status == StorageMessages.RetrieveChunk.status.CORRUPT) {
                            continue;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
