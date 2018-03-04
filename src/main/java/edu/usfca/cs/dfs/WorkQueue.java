package edu.usfca.cs.dfs;
import java.util.LinkedList;

/**
 * Created By: Melanie Baybay
 */
public class WorkQueue {
    /** ---------------------------------- DATA ATTRIBUTES ---------------------------------- **/
    private final int DEFAULT_NUM_THREADS = 10;
    private final PoolWorker[] threads;
    private final LinkedList<Runnable> queue;
    private volatile boolean acceptingTasks;
    private int nThreads;
    /** ------------------------------------------------------------------------------------- **/

    /** ------------------------------------ CONSTRUCTOR ------------------------------------ **/
    public WorkQueue() {
        this.nThreads = DEFAULT_NUM_THREADS;
        this.threads = new PoolWorker[DEFAULT_NUM_THREADS];
        this.queue = new LinkedList<>();
        for(int i=0; i < DEFAULT_NUM_THREADS; i++) {
            threads[i] = new PoolWorker();
            threads[i].start();
        }
        this.acceptingTasks = true; // start accepting tasks
    }
    public WorkQueue(int nThreads) {
        this.nThreads = nThreads;
        this.threads = new PoolWorker[nThreads];
        this.queue = new LinkedList<>();
        for(int i=0; i < nThreads; i++) {
            threads[i] = new PoolWorker();
            threads[i].start();
        }
        this.acceptingTasks = true; // start accepting tasks
    }
    /** ------------------------------------------------------------------------------------- **/

    /** ----------------------------------- PUBLIC METHODS ---------------------------------- **/
    public void execute(Runnable task) {
        if(acceptingTasks) {
            synchronized(queue) {
                queue.addLast(task);
                queue.notify();
            }
        }
    }

    public void shutdown() {
        this.acceptingTasks = false; // close queue from incoming tasks
        synchronized(queue) {
            queue.notifyAll();
        }
    }

    public void close() {
        for (int i = 0; i < this.nThreads; i++) {
            try {
                threads[i].interrupt();
                threads[i].join();
            } catch (InterruptedException ignore) {
            }
        }
    }
    /** ------------------------------------------------------------------------------------- **/

    private class PoolWorker extends Thread {
        public void run() {
            Runnable r;
            while(true) {
                synchronized(queue) {
                    try {
                        queue.wait();
                    } catch(InterruptedException ignore) {
                        break;
                    }
                }
                r = queue.removeFirst();

                try {
                    r.run();
                } catch(RuntimeException e) {
                    //TODO: add logger message
                    e.printStackTrace();
                }
            }
        } /* --- end of run() -- */
    }

} /* --- WorkQueue class --- */
