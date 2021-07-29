package org.apache.curator.framework.recipes;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.RevocationListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.QuorumConfigBuilder;
import org.apache.curator.test.TestingZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CuratorRecipeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorRecipeTest.class);
    private static String LOCK_PATH = "/test/lock";
    private static long QUITE_PERIOD = 5000;
    private static long WAIT_PERIOD_BEFORE_DELETE_ZNODE= 10000;
    private static long EXTENDED_QUITE_PERIOD = 120000;
    private static int NUMBER_OF_THREADS = 2;
    private static int SESSION_TIMEOUT = 30000;
    private static int CONNECTION_TIMEOUT = 30000;
    private static int RETRIES = 1;
    private static int SLEEP_BETWEEN_RETRIES = 100;
    private static RetryPolicy RETRYPOLICY = new RetryNTimes(RETRIES, SLEEP_BETWEEN_RETRIES);
    /*
        delete lock nodes to simulate the state where the zkClient had reconnected and it still happens
        to see the ephermal node just before the server deletes it since its session has expired,
        but the node is deleted afterwards by the server.
     */
    private static boolean SIMULATE_ISSUE_BY_EXPLICIT_DELETE = true;
    private static boolean ADD_REVOCABLE_LISTENER = false;
    private CuratorFramework client;
    private TestingZooKeeperServer zkTestServer;

    @Test
    public void testInterProcessMutexLock() throws Exception{
        zkTestServer = new TestingZooKeeperServer(new QuorumConfigBuilder(InstanceSpec.newInstanceSpec()));
        zkTestServer.start();
        client = CuratorFrameworkFactory.newClient(zkTestServer.getInstanceSpec().getConnectString(),SESSION_TIMEOUT, CONNECTION_TIMEOUT, RETRYPOLICY);

        BackgroundThread[] BackgroundThreads = new BackgroundThread[NUMBER_OF_THREADS];
        for (int i = 0; i < BackgroundThreads.length; i++) {
            BackgroundThreads[i] = new BackgroundThread(i);
            BackgroundThreads[i].start();
        }
        try {
            client.start();
            if(SIMULATE_ISSUE_BY_EXPLICIT_DELETE){
                Thread.sleep(WAIT_PERIOD_BEFORE_DELETE_ZNODE);
                /*
                    delete lock nodes to simulate the state where the zkClient had reconnected and it still happens
                    to see the ephermal node just before the server deletes it since its session has expired,
                    but the node is deleted afterwards by the server.
                 */
                LOGGER.info("MAIN Thread: Deleting lock nodes");
                deleteLockNodes();
            }
            Thread.sleep(EXTENDED_QUITE_PERIOD);
        } catch (Exception e) {
            LOGGER.info("MAIN EXCEPTION" + e.getMessage());
        } finally {
            LOGGER.info("Main thread : Cleaning up all resources");
            for (int i = 0; i < BackgroundThreads.length; i++) {
                BackgroundThreads[i].close();
                BackgroundThreads[i].stopRunning();
            }
            client.close();
            zkTestServer.close();
        }
    }

    private void deleteLockNodes() throws Exception{
        List<String> children = (List)client.getChildren().forPath(LOCK_PATH);
        for(String child : children){
            String childPath = LOCK_PATH + "/" + child;
            this.client.delete().guaranteed().forPath(childPath);
        }
    }

    private class BackgroundThread extends Thread {
        private boolean stopRunning = false;
        private final CuratorFramework cf;
        private final InterProcessReadWriteLock readWriteLock;
        private final InterProcessMutex writeMutex;
        private final AtomicBoolean canHoldLock = new AtomicBoolean(true);
        private String logPrefix;
        private String logMonitorPrefix;

        BackgroundThread(int id) {
            logPrefix = "INSTANCE "+ id + ": ";
            logMonitorPrefix = logPrefix + " : [Lock Monitor] : ";
            cf = CuratorFrameworkFactory.newClient(zkTestServer.getInstanceSpec().getConnectString(),SESSION_TIMEOUT, CONNECTION_TIMEOUT, RETRYPOLICY);
            readWriteLock = new InterProcessReadWriteLock(cf, LOCK_PATH);
            writeMutex = readWriteLock.writeLock();
            if(ADD_REVOCABLE_LISTENER){
                addLockRevocableListener();
            }
            addConnectionStateListener();
        }

        @Override
        public void run() {
            try {
                // starting curator client
                cf.start();

                // lock monitor process starts - stops only when stopRunning is set to true (signal from MAIN THREAD / EXCEPTION)
                while(!stopRunning){
                    // first thing we do is to try to release a potentially held lock acquired in a previous iteration.
                    releaseLock(writeMutex);

                    LOGGER.info(logMonitorPrefix + "Attempting to acquire lock");
                    writeMutex.acquire();

                    // if it reaches this point, it's because the thread now owns the zk lock
                    LOGGER.info(logMonitorPrefix + "Lock acquired");

                    // after holding the lock, goes to wait, react to signal (canHoldLock.notify()) from [Connection Event SUSPENDED / LOST]
                    synchronized(canHoldLock){
                        if (canHoldLock.get()) {
                            LOGGER.info(logMonitorPrefix + "waits until connection is suspended / lost");
                            canHoldLock.wait();
                        }
                    }
                    LOGGER.info(logMonitorPrefix + "can no longer hold the lock");
                    releaseLock(writeMutex);
                }

            } catch (Exception e) {
                LOGGER.info(logMonitorPrefix + "Exit due to exception " + e.getMessage());
                stopRunning = true;
                cf.close();
            }
        }

        public void releaseLock(InterProcessMutex writeMutex) throws Exception{
            if(writeMutex.isAcquiredInThisProcess()){
                LOGGER.info(logMonitorPrefix + "Attempting to release lock after 5s wait");
                Thread.sleep(QUITE_PERIOD);
                LOGGER.info(logMonitorPrefix + "Releasing lock now");
                writeMutex.release();
                LOGGER.info(logMonitorPrefix +"Released lock");
            }
        }


        public void close(){
            cf.close();
        }

        public void stopRunning() {
            stopRunning = true;
        }

        /**
         * This listener gets fired when its respective lock nodes gets deleted (NodeDeleted event) / changed (NodeDataChanged)
         * This enables application to revoke the lock if its respective lock nodes gets deleted
         */
        public void addLockRevocableListener(){
            RevocationListener revocationListener = new RevocationListener() {
                @Override
                public void revocationRequested(Object o) {
                    try {
                        synchronized(canHoldLock){
                            LOGGER.info(logPrefix + "Notifying lock monitor to release the lock since its lock node get deleted");
                            canHoldLock.notify(); // notify lock monitor so as it can release its lock if acquired otherwise wouldn't be notified
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            writeMutex.makeRevocable(revocationListener);
        }

        public void addConnectionStateListener(){
            ConnectionStateListener csl = new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    String listenerLogPrefix = logPrefix + " : [Connection Listener Event] : ";
                    try{
                        switch (newState.toString()){
                            case "SUSPENDED":{
                                LOGGER.info(listenerLogPrefix + "Connection suspended");
                                synchronized(canHoldLock){
                                    if (canHoldLock.compareAndSet(true, false)) {
                                        LOGGER.info(listenerLogPrefix + "Notifying lock monitor to release the lock since SUSPENDED");
                                        canHoldLock.notify(); // notify lock monitor so as it can release its lock if acquired otherwise wouldn't be notified
                                    }
                                }
                                break;
                            }
                            case "LOST":{
                                LOGGER.info(listenerLogPrefix + "Connection lost");
                                synchronized(canHoldLock){
                                    if (canHoldLock.compareAndSet(true, false)) {
                                        LOGGER.info(listenerLogPrefix + "Notifying lock monitor to release the lock since LOST");
                                        canHoldLock.notify(); // notify lock monitor so as it can release its lock if acquired otherwise wouldn't be notified
                                    }
                                }
                                break;
                            }
                            case "RECONNECTED":{
                                LOGGER.info(listenerLogPrefix + "Connection reconnected with session id : 0x" + Long.toHexString(cf.getZookeeperClient().getZooKeeper().getSessionId()) );
                                canHoldLock.set(true);
                                break;
                            }
                            case "CONNECTED":{
                                LOGGER.info(listenerLogPrefix + "Connected with session id : 0x" + Long.toHexString(cf.getZookeeperClient().getZooKeeper().getSessionId()));
                                canHoldLock.set(true);
                                break;
                            }
                        }
                    } catch (Exception e){
                        LOGGER.info(listenerLogPrefix + e.getMessage());
                        stopRunning = true;
                        cf.close();
                    }
                }
            };
            cf.getConnectionStateListenable().addListener(csl);
        }
    };
}
