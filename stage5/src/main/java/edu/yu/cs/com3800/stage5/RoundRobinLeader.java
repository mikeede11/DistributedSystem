package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    private AtomicInteger nextWorker;
    private final ZooKeeperPeerServerImpl server;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<Long> servers;
    private Logger leaderLog;

    //This boolean will enable a newly elected leader
    // to sleep for some time before it starts trying
    // to connect via TCP to followers who may not be ready yet (didn't finish election).
    // In truth this may not be necessary anymore since I designed it such that all followers
    // can technically periodically check for connections during an election
    private boolean justElected;
    private ServerSocket tcpServer;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, ConcurrentHashMap<Long, InetSocketAddress> book, ArrayList<Long> servers, AtomicInteger nextWorker, ServerSocket tcpServer) {
        this.nextWorker = nextWorker;
        this.server = server;
        this.peerIDtoAddress = book;
        this.servers = servers;
        this.justElected = true;
        this.tcpServer = tcpServer;
    }

    @Override
    public void run() {
        try {
            leaderLog = initializeLogging(this.getClass().getCanonicalName() + "-with-port-" + server.getUdpPort(), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ExecutorService executor = Executors.newCachedThreadPool();
        Socket tcpClient = null;
        /*This will only be false (generally) when shutdown is called in the server.
        * This makes sense b/c once a leader becomes leader it should be forever
        * (unless server goes down or explicitly stopped w/ an interrupt)
         */
        while (!Thread.currentThread().isInterrupted()) {
            try {
                tcpServer.setSoTimeout(5000);
                tcpClient = tcpServer.accept();//RRLeader accepts gateway requests at UDP port + 2
                leaderLog.info("Received gateway request");
                //System.out.println("Received gateway request");
                /*give this thread this socket (tcpClient) which is now connected to the gateway.
                * Each task submitted to this executor will be performed asynchronously.
                * This enables a leader to process many client requests concurrently
                * */
                executor.submit(new RRLHelper(server,tcpClient, justElected));
                if(justElected){
                    justElected = false;//purpose explained above
                }
            } catch (IOException e) {
              //e.printStackTrace();
               // System.out.println("Leader Timed out");
            }
        }
        executor.shutdownNow();
    }
}
