package org.apache.curator.framework.recipes;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class CuratorLeaderLatchRecipeTest {

    public static void main(String[] args){
        try{
            new CuratorLeaderLatchRecipeTest().testLeaderLatch();
        }catch (Exception e){

        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorLeaderLatchRecipeTest.class);
    private static String LOCK_PATH = "/test/lock";
    private static long QUITE_PERIOD = 5000;
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
    private CuratorFramework client;

    public TestingZooKeeperServer zkTestServer;

    public void setup() throws Exception {
        zkTestServer = new TestingZooKeeperServer(new QuorumConfigBuilder(InstanceSpec.newInstanceSpec()));
        zkTestServer.start();
        client = CuratorFrameworkFactory.newClient(zkTestServer.getInstanceSpec().getConnectString(),SESSION_TIMEOUT, CONNECTION_TIMEOUT, RETRYPOLICY);
    }

    @Test
    public void testLeaderLatch() throws Exception{
        setup();
        final BackgroundThread[] BackgroundThreads = new BackgroundThread[NUMBER_OF_THREADS];
        for (int i = 0; i < BackgroundThreads.length; i++) {
            BackgroundThreads[i] = new BackgroundThread(i);
            BackgroundThreads[i].start();
            Thread.sleep(5000);
        }
        Thread.sleep(10000);

        triggerReconnectEvents(BackgroundThreads);

        LeaderLatch leaderThreadLeaderLatchInstance = BackgroundThreads[0].getLeaderLatch();
        LeaderLatch nonLeaderThreadLeaderLatchInstance = BackgroundThreads[1].getLeaderLatch();
        while(leaderThreadLeaderLatchInstance.nonLeaderThreadWaitLatch.getCount() != 0){
            Thread.sleep(1);
        }
        nonLeaderThreadLeaderLatchInstance.nonLeaderThreadWaitLatch.countDown();
        while(nonLeaderThreadLeaderLatchInstance.leaderThread2WaitLatch1.getCount() != 0){
            Thread.sleep(1);
        }
        leaderThreadLeaderLatchInstance.leaderThread2WaitLatch1.countDown();

        Thread.sleep(100000);
        for (int i = 0; i < BackgroundThreads.length; i++) {
            BackgroundThreads[i].close();
            BackgroundThreads[i].stopRunning();
        }
        client.close();
        zkTestServer.close();
    }

    private void triggerReconnectEvents(final BackgroundThread[] backgroundThreads) throws Exception{
        new Thread(new Runnable() {
            @Override
            public void run() {
                LeaderLatch leaderLatch = backgroundThreads[1].getLeaderLatch();
                CountDownLatch fireLatch = backgroundThreads[1].getFireLatch();
                leaderLatch.fireReconnectEvent();
                fireLatch.countDown();
            }
        }, "non-leader-thread").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                LeaderLatch leaderLatch = backgroundThreads[0].getLeaderLatch();
                CountDownLatch fireLatch = backgroundThreads[0].getFireLatch();
                leaderLatch.fireReconnectEvent();
            }
        }, "leader-thread-1").start();

        Thread.sleep(10000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                LeaderLatch leaderLatch = backgroundThreads[0].getLeaderLatch();
                leaderLatch.fireReconnectEvent();
            }
        }, "leader-thread-2").start();
    }
    private class BackgroundThread extends Thread {
        private boolean stopRunning = false;
        private final CuratorFramework cf;
        private final LeaderLatch leaderLatch;
        private final CountDownLatch fireLatch = new CountDownLatch(1);
        private String logPrefix;
        private String logMonitorPrefix;

        public LeaderLatch getLeaderLatch() {
            return leaderLatch;
        }

        public CountDownLatch getFireLatch() {
            return fireLatch;
        }

        BackgroundThread(int id) {
            logPrefix = "INSTANCE "+ id + ": ";
            logMonitorPrefix = logPrefix + " : [Lock Monitor] : ";
            cf = CuratorFrameworkFactory.newClient(zkTestServer.getInstanceSpec().getConnectString(),SESSION_TIMEOUT, CONNECTION_TIMEOUT, RETRYPOLICY);
            leaderLatch = new LeaderLatch(cf, LOCK_PATH);
            addLeaderLatchListener();
        }

        @Override
        public void run() {
            try {
                // starting curator client
                cf.start();
                leaderLatch.start();

            } catch (Exception e) {
                LOGGER.info(logMonitorPrefix + "Exit due to exception " + e.getMessage());
                e.printStackTrace();
                stopRunning = true;
                cf.close();
            }
        }

        public void close(){
            cf.close();
        }

        public void stopRunning() {
            stopRunning = true;
        }

        public void addLeaderLatchListener(){
            LeaderLatchListener l1 = new LeaderLatchListener() {
                String listenerLogPrefix = logPrefix + " : [Leader Listener Event] : ";
                @Override
                public void isLeader() {
                    LOGGER.info(listenerLogPrefix + "Has become the leader");
                }

                @Override
                public void notLeader() {
                    LOGGER.info(listenerLogPrefix + "Lost a leadership");
                }
            };
            leaderLatch.addListener(l1);
        }
    };
}
