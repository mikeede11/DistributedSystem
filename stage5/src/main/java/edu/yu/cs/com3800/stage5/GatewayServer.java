package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class GatewayServer implements LoggingServer {
    private HttpServer server;
    private final InetSocketAddress addr;
    private Logger myLog;
    private AtomicInteger leaderPort = new AtomicInteger();
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private final ZooKeeperPeerServerImpl gwpsi;
    private ArrayList<Thread> threads;
    private AtomicInteger  reqID;
    private AtomicBoolean electionInProgress;

    public GatewayServer(int serverPort, long peerEpoch, Long id, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress){
        this.addr = new InetSocketAddress("localhost", serverPort);
        this.peerIDtoAddress = peerIDtoAddress;
//        this.threads = servers;
        this.gwpsi = new GatewayPeerServerImpl(serverPort, peerEpoch, id, peerIDtoAddress);
       Thread t = new Thread(gwpsi, "GatewayServer-on-port" + serverPort);
        t.start();
//        threads.add(t);//USELESS
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
        }
        try {
            myLog = initializeLogging(this.getClass().getCanonicalName() + "-with-port-" + serverPort, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.reqID = new AtomicInteger(0);
        this.electionInProgress = new AtomicBoolean(false);//this boolean variable is for all elections except for the initial one.
    }

    public void start(){
        try {
            //gwpsi.activateElectionUpdateService();//NEW
            server = HttpServer.create(addr, 500);
            server.createContext("/compileandrun", new MyHandler());
            server.createContext("/getserverstatus", new StatusUpdater());
            server.createContext("/GossipLogs", new GossipLogHandler());
            server.setExecutor(Executors.newCachedThreadPool()); // creates a default executor
            server.start();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    public ZooKeeperPeerServerImpl getGwpsi() {
        return gwpsi;
    }

    public synchronized void checkIfLeaderDeadToStartElection(){
        if(gwpsi.isPeerDead(gwpsi.getCurrentLeader().getProposedLeaderID())  && gwpsi.getCurrentLeader().getProposedLeaderID() != gwpsi.getServerId() && !electionInProgress.get()){
            //can be concurrency error between condition check and setting over here
            electionInProgress.set(true);
            //Once you have a new leader this will test false, but you should wait till end of election
            myLog.info("Gateway noticed " + gwpsi.getCurrentLeader().getProposedLeaderID() + " is dead!");
            //System.out.println(gwpsi.getCurrentLeader().getProposedLeaderID() + " != " + gwpsi.getServerId());
            //System.out.println("BGateway noticed " + gwpsi.getCurrentLeader().getProposedLeaderID() + " is dead! and my ID is " + gwpsi.getServerId());
            //System.out.println(gwpsi.getCurrentLeader().getProposedLeaderID() + " != " + gwpsi.getServerId());
            try {
                gwpsi.setCurrentLeader(new Vote(gwpsi.getServerId(), gwpsi.getPeerEpoch() + 1));// + 1 b/c your triggering a new election
            } catch (IOException e) {
                e.printStackTrace();
            }
            gwpsi.incrPeerEpoch();

        }
    }
    class MyHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {//takes client request
            myLog.info("Client HTTP request connected");
            boolean resultSentToClient = false;
            InputStream is = t.getRequestBody();
            byte[] clientreqcode = is.readAllBytes();

            while(!resultSentToClient) {
                if(gwpsi.getCurrentLeader().getProposedLeaderID() == gwpsi.getServerId()) {
                    //In main thread an election should start - by def it will and if election works then this will eventually end
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                electionInProgress.set(false);
                Message msgToLeader = new Message(Message.MessageType.WORK, clientreqcode, addr.getHostString(), addr.getPort() + 2, peerIDtoAddress.get(gwpsi.getCurrentLeader().getProposedLeaderID()).getHostString(), peerIDtoAddress.get(gwpsi.getCurrentLeader().getProposedLeaderID()).getPort() + 2, reqID.get());

                leaderPort.set(peerIDtoAddress.get(gwpsi.getCurrentLeader().getProposedLeaderID()).getPort());
                myLog.info("connecting to the Leader at tcp port " + (leaderPort.get() + 2));
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
               // System.out.println("Gateway with id " + gwpsi.getServerId() + " attempting connecting to the Leader at tcp port " + (leaderPort.get() + 2));
                try {
                    //try to connect to leader and send req. if success continue. if fail go to catch
                    Socket s = new Socket("localhost", leaderPort.get() + 2);
                    myLog.info("connection successful");
                    OutputStream oos = s.getOutputStream();
                    oos.write(msgToLeader.getNetworkPayload());//writes code to leader/
                    myLog.info("Sent this message to the Leader: " + new String(clientreqcode));
                   // System.out.println("Sent this message to the Leader: " + new String(clientreqcode));
                    InputStream ois = s.getInputStream();

                    if (Util.bytesReadableAndPeerisAlive(ois, gwpsi, peerIDtoAddress.get(gwpsi.getCurrentLeader().getProposedLeaderID()))) {
                        //theres something in the buffer and the leader is alive. try to read response and send back to client
                        //if it fails we will end up in the catch. if it succeeds we will set  boolean and exit..
                        //this also means we will not accept a message from a failed leader/ Like even if leader sends back something were
                        //not supposed to accept it if its considered dead. this ensures no faulty responses.
                        byte[] result = Util.readAllBytes(ois);
                        myLog.info("Gateway received response from Leader!");
                        //System.out.println("Gateway received response from Leader!");
                        Message msgFromLeader = new Message(result);
                        byte[] codeResult = msgFromLeader.getMessageContents();
                        t.sendResponseHeaders(200, codeResult.length);
                        OutputStream os = t.getResponseBody();
                        os.write(codeResult);//send back to client
                        resultSentToClient = true;
                        reqID.getAndIncrement();//for next request;
                        os.close();
                        s.close();
                        oos.close();
                        ois.close();//We are client to leader, we can close socket and streams to it. Closing socket also closes associated streams.
                    }else if(gwpsi.isPeerDead(gwpsi.getCurrentLeader().getProposedLeaderID()) && gwpsi.getCurrentLeader().getProposedLeaderID() != gwpsi.getServerId() && !electionInProgress.get()){//but im not dead during election
                        myLog.info("Gateway noticed " + gwpsi.getCurrentLeader().getProposedLeaderID() + " is dead!");
                        //System.out.println("AGateway noticed " + gwpsi.getCurrentLeader().getProposedLeaderID() + " is dead! and my ID is " +gwpsi.getServerId());
                        gwpsi.setCurrentLeader(new Vote(gwpsi.getServerId(), gwpsi.getPeerEpoch() + 1));// + 1 b/c your triggering a new election
                        gwpsi.incrPeerEpoch();
                    }
                }catch(IOException | NullPointerException e ){
                    checkIfLeaderDeadToStartElection();
                }
            }
        }
    }

    class StatusUpdater implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {//takes client request
            if (gwpsi.getServerId() == gwpsi.getCurrentLeader().getProposedLeaderID()) {
                byte[] codeResult = "ELECTION_IN_PROGRESS".getBytes();
                t.sendResponseHeaders(200, codeResult.length);
                OutputStream os = t.getResponseBody();
                os.write(codeResult);//send back to client
            } else {
                StringBuilder str = new StringBuilder((gwpsi.getCurrentLeader().getProposedLeaderID() + "=LEADING " + gwpsi.getServerId() + "=OBSERVER "));
                for (Long id : peerIDtoAddress.keySet()) {
                    if (id != gwpsi.getCurrentLeader().getProposedLeaderID() && id != gwpsi.getServerId()) {
                        str.append(id).append("=FOLLOWING ");
                    }
                }
                byte[] codeResult = str.toString().getBytes();
                t.sendResponseHeaders(200, codeResult.length);
                OutputStream os = t.getResponseBody();
                os.write(codeResult);//send back to client
            }
        }
    }

    class GossipLogHandler implements HttpHandler {
        /*        This code will provide the HTTP service that
                 returns the list of gossip messages for a server
                 in a log file. (It handles the request)
                 */
        public void handle(HttpExchange t) throws IOException {//takes client request
            try {
                //convert the list to a log file
                Logger gossipLog = gwpsi.getGossipMessageLog();
                try {
                    gossipLog = initializeLogging("Server-with-port-" + gwpsi.getUdpPort() + "-gossip-message-list", false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                StringBuilder gossipListString = new StringBuilder();
                for (int i = 0; i < gwpsi.getGossipMessageList().size(); i++) {
                    gossipLog.info(gwpsi.getGossipMessageList().get(i).toString());
                    gossipListString.append(gwpsi.getGossipMessageList().get(i).toString());
                }
                //signal to client youre done - client should now have acces to that file
                //how do we send list over http. ????
                byte[] result = gossipListString.toString().getBytes();
                t.sendResponseHeaders(200, result.length);
                OutputStream os = t.getResponseBody();
                os.write(result);//send back to client
            } catch (IOException | NullPointerException e) {
            }
        }
    }



}