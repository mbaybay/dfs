package edu.usfca.cs.dfs;

import java.util.concurrent.TimeUnit;

/**
 * Created By: Melanie Baybay
 */
public class Driver {
    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public static void main(String[] args) {
        Client client = new Client(args[0], Integer.parseInt(args[1]));
        client.start();
        try {
//            client.storeFile("/home4/mbaybay/resources/test/test_file_1.bin");
//            TimeUnit.SECONDS.sleep(3);
//            client.storeFile("/home4/mbaybay/resources/test/test_file_2.bin");
//            TimeUnit.SECONDS.sleep(3);
//            client.storeFile("/home4/mbaybay/resources/test/test_file_3.bin");
//            client.getStorageSummary();
//            TimeUnit.SECONDS.sleep(10);
//            client.getFileList();
//            TimeUnit.SECONDS.sleep(5);
            client.retrieveFile("/home4/mbaybay/resources/test/test_file_1.bin");
            TimeUnit.SECONDS.sleep(3);
            client.retrieveFile("/home4/mbaybay/resources/test/test_file_3.bin");
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.shutdown();
        System.exit(0);
    }
    /** ------------------------------------------------------------------------------------- **/
}
