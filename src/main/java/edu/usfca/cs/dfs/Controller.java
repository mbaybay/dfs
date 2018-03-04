package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.*;

public class Controller {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private final static Logger logger = Logger.getLogger(Controller.class.getName());
    private final static String BASEDIR = "/home2/mbaybay/";
    private final int DEFAULT_NUM_THREADS = 10;
    private final int DEFAULT_NUM_REPLICAS = 3;
//    private static ConsoleHandler ch = null;
//    private FileHandler fh = null;
    private boolean alive;
    private ServerSocket sock;
    private WorkQueue requestQueue;
    private ControllerData data;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    public Controller() {
        this.alive = true;
        this.data = new ControllerData(DEFAULT_NUM_REPLICAS);
        this.requestQueue = new WorkQueue(DEFAULT_NUM_THREADS);
    }
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public static void main(String[] args) throws Exception{
        // initializing LOGGER
        try {
            File dir = new File(BASEDIR + "logs/");
            if (!dir.isDirectory()) {
                dir.mkdir();
            }
            FileHandler fh = new FileHandler(dir.getPath() + "Controller.log");
            logger.addHandler(fh);
            logger.log(Level.INFO, "FileHandler: " + dir.getPath() + "Controller.log");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Unable set Controller log file: " + e.toString());
        }
        logger.setLevel(Level.INFO);
        logger.log(Level.INFO, "Starting controller...");
        new Controller().start();
    }

    private void start() {
        try {
            int port = new PortManager().getAvailablePort();
            if (port != -1) {
                this.sock = new ServerSocket(port);
                logger.log(Level.INFO, "Controller Listening on " + getHostname() + " " + getPort() + "...");
                while (true) {
                    Socket socket = sock.accept();
                    if (socket.isConnected()) {
                        requestQueue.execute(new ControllerWorker(socket, data));
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

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
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PRIVATE METHODS --------------------------------- **/
    /** ------------------------------------------------------------------------------------- **/

}
