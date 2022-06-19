package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {


    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;

    //I maintain two pairs of messaging queues - one for UDP election messages and the other for UDP gossip messages.
    //I did this because I felt it was conceptually cleaner and avoids the overhead (and waste)
    // of threads dealing with messages they are not supposed to deal with.
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> gossipOut;
    private LinkedBlockingQueue<Message> gossipIn;

    private ConcurrentHashMap<Long, Gossip> peerIDToGossipData;//this contains the peer server's gossip data

    //this contains a list of all the servers in the cluster (except myself).
    // If their address -> ID is > 0 it means its alive. If a peer server's ID is -1, it means it's dead.
    private ConcurrentHashMap<InetSocketAddress, Long> deadPeersList;

    private Long id;
    private long observerID;//May be unnecessary
    private long peerEpoch;
    private volatile Vote currentLeader;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress;//list of peers in this cluster except myself, highly used
    private ArrayList<Long> servers;//list of all servers except for myself. Used to easily implement round robin work distribution. I felt a map wouldn't have been clean enough.
    private AtomicInteger nextWorker;//the integer that is used as an index to get the worker ID from the list above. synchronized between leading threads (hopefully :)

    //the pendingMessages list's purpose depends on your state
    //for a Leader - it will be the DS that gathers last work for a newly elected leader.
    //for a follower - CompletedWork not yet sent to leader (while waiting for new election results)
    //No purpose for Gateway.
    private ArrayList<Message> pendingMessages;//will contain messages that a server hasn't fully processed yet

    static final int GOSSIP = 3000;
    static final int FAIL = GOSSIP * 6;
    static final int CLEANUP = FAIL * 2;

    private Thread senderWorker;//UDPMR/S thread references so we can shutdown these daemon threads when need be
    private Thread receiverWorker;
    private Thread gossipSender;
    private Thread gossipReceiver;
    private boolean rrlStarted;//prevents a server from creating infinite leader threads if it is LEADING
    private boolean jrfStarted;// or infinite follower threads if it is FOLLOWING
    HashMap<Long, InetSocketAddress> clientAddresses;//May be unnecessary
    Logger log;
    private Logger gossipMessageLog;//for HTTP service
    private Thread gossiper;//thread references so we can shutdown these daemon threads when need be
    private Thread stethoscope;
    private Thread rrlThread;
    private Thread jrfThread;
    private boolean signaledToShutdown;//use for testing purposes
    private ServerSocket tcpServer;//a server socket. A follower will use this to accept work orders from leader. A leader from a gateway.
    private ArrayList<GossipMessageForHTTP> gossipMessageList;//list for HTTP service

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress){
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerIDToGossipData = new ConcurrentHashMap<>();
        this.deadPeersList = new ConcurrentHashMap<>();
        this.myAddress = new InetSocketAddress("localhost", this.myPort);
        this.nextWorker = new AtomicInteger(1);//Avoid observer
        this.observerID = -1;//unnecessary
        this.currentLeader = new Vote(this.id, this.peerEpoch);//start with yourself for election
        this.state = ServerState.LOOKING;//start off looking so we have election so we can determine leader so we can coordinate.
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.gossipIn = new LinkedBlockingQueue<>();
        this.gossipOut = new LinkedBlockingQueue<>();
        this.servers = new ArrayList<>(peerIDtoAddress.size());//- 1 for observer and maybe leader too?
        this.pendingMessages = new ArrayList<>();
        this.clientAddresses = new HashMap<>();//unnecessary
        Iterator<Long> tempSet = peerIDtoAddress.keySet().iterator();

        //The subsequent code fills our Data structures with their initial logical values
        for(int i = 0;tempSet.hasNext();i++){
            Long next = tempSet.next();
            if(currentLeader.getProposedLeaderID() != next) {//TODO init i at 1 to skip observer because observer is not a worker - this isnt a sustainable solution though.
                servers.add(i, next);
            }
        }


        for(Long peerID: peerIDtoAddress.keySet()){
            peerIDToGossipData.put(peerID, new Gossip(0, System.currentTimeMillis()));
        }
        peerIDToGossipData.put(id, new Gossip(0, System.currentTimeMillis()));//we have to add our own gossip data b/c were not in the orig. map b/c we don't want to be sending messages to ourselves in election or what not

        for(Map.Entry<Long, InetSocketAddress> entry: peerIDtoAddress.entrySet()){
            deadPeersList.put(entry.getValue(), entry.getKey());
            deadPeersList.put(new InetSocketAddress(entry.getValue().getHostString(), entry.getValue().getPort() + 2), entry.getKey());//for TCP
            deadPeersList.put(new InetSocketAddress(entry.getValue().getHostString(), entry.getValue().getPort() + 4), entry.getKey());//to account for the other port used for gossip UDP
        }

        try {
            this.tcpServer = new ServerSocket(myPort + 2);//for a follower node to accept leader requests and a Leader to accept gateway requests
            this.gossipMessageList = new ArrayList<>();
            log = initializeLogging(this.getClass().getCanonicalName() + "-with-port-" + this.myPort , false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown(){
        //for some reason the shutdown method wouldn't shut them down.
        // This should not be such an issue though because from what
        // I understand this method is deprecated due to deadlock issues.
        // However this methods goal is to stop execution of the ENTIRE server so we are not looking for liveness anyways.
        this.receiverWorker.stop();
        this.senderWorker.stop();
        this.gossipSender.stop();
        this.gossipReceiver.stop();
        ((Gossiper)(this.gossiper)).shutdownGossiper();
        ((Stethoscope)(this.stethoscope)).shutdownStethoscope();
        if(state == ServerState.FOLLOWING){
            this.jrfThread.interrupt();
        }
        else if(state == ServerState.LEADING){
            this.rrlThread.interrupt();
        }
        this.shutdown = true;
    }

    @Override
    public void run(){
        //step 1: create and run threads that sends election messages and gossip messages respectively
        senderWorker = Util.startAsDaemon( new UDPMessageSender(this.outgoingMessages, this.myPort), "sender");
        gossipSender = Util.startAsDaemon(new UDPMessageSender(this.gossipOut, this.myPort + 4), "GossipSender");

        //step 2: create and run thread that listens for election and gossip messages sent to this server, respectively.
        try {
            receiverWorker = Util.startAsDaemon(new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this), "receiver");
            gossipReceiver = Util.startAsDaemon(new UDPMessageReceiver(this.gossipIn, new InetSocketAddress("localhost", this.myPort+4), this.myPort + 4, this), "GossipReceiver");
        } catch (IOException e) {
            e.printStackTrace();
        }


        //make thread that speaks gossip (increments a server's own heartbeat and uses gossipSender to send that out.)
        gossiper = new Gossiper(this, gossipOut, peerIDtoAddress.values().toArray(new InetSocketAddress[peerIDtoAddress.size()]), peerIDToGossipData);
        Util.startAsDaemon(gossiper, "PS-" + id + "-Gossiper");
        //make thread that listens to gossip - processes received gossip messages and updates its server's gossip data (stethoscope - because it listens for heartbeats :))
        stethoscope = new Stethoscope(this, gossipIn, peerIDToGossipData, peerIDtoAddress, servers, deadPeersList, log);
        Util.startAsDaemon(stethoscope, "PS-" + id + "-Stethoscope");

        //step 3: main server loop
        try{
            while (!this.shutdown && !this.isInterrupted()){

                Thread.sleep(500);//slow down a bit we do not expect the state to be changing every millisecond
                switch (getPeerState()){
                    case LOOKING:
                        rrlStarted = false;//for a follower that entered election and became leader - may be unnecessary
                        Thread.sleep(10000);//WAIT FOR EVERYONE TO GET IN ELECTION STATE SO WE DONT MISS ANY VOTES
                        ZooKeeperLeaderElection ZKLE = new ZooKeeperLeaderElection(this, this.incomingMessages);
                        ZKLE.lookForLeader();//start leader election
                        incomingMessages.clear();//MAYBE UNNECESSARY
                        break;
                    case FOLLOWING:
                        if(!jrfStarted) {//if we do not already have a JavaRunnerFollower running and we are a follower - start it up!
                            jrfThread = Util.startAsDaemon(new JavaRunnerFollower(this, tcpServer), "JRF Thread");
                            jrfStarted = true;//so we do not infinite loop follower thread creation.
                        }

                        break;
                    case LEADING:
                        if(!rrlStarted){
                                //remove my id which is the leader from list of servers that are workers
                                for (int i = 0; i < servers.size(); i++) {
                                    if (servers.get(i).equals(id)) {
                                        servers.remove(i);
                                    }
                                }
                                //collect Old  messages from all followers by sending messages to them all
                                for (int i = 1; i < servers.size(); i++) {//start at one to ignore Observer
                                    InetSocketAddress workerAddr = peerIDtoAddress.get(servers.get(i));//get worker address
                                    try {
                                        Socket tcpConnect = new Socket(workerAddr.getHostString(), workerAddr.getPort() + 2);//connect with that address
                                        OutputStream streamToWorker = tcpConnect.getOutputStream();
                                        if (!isPeerDead(workerAddr)) {//if the server is not dead - ask for last work (this test may be ridiculous - if the server is in the list of servers then it must be alive?! it could become dead between beg. of loop and now)
                                            streamToWorker.write(new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, new byte[]{}, myAddress.getHostString(), myPort + 2, workerAddr.getHostString(), workerAddr.getPort() + 2).getNetworkPayload());
                                        } else {//otherwise go to next worker. just ignore that work (if there was) it is lost the gateway will request it and well make new request
                                            continue;
                                        }
                                        InputStream lastWorkStream = tcpConnect.getInputStream();
                                        /*get response from worker*/
                                        if (Util.bytesReadableAndPeerisAlive(lastWorkStream, this, workerAddr)) {//if this is false then just ignore that work it is lost the gateway will request it and we'll make new request (from gateway)
                                            byte[] result = Util.readAllBytes(lastWorkStream);
                                            Message msgFromWorker = new Message(result);
                                            if (msgFromWorker.getRequestID() != -1) {
                                                pendingMessages.add(msgFromWorker);
                                            }//otherwise ignore
                                        }
                                    }catch (IOException e){
                                        //just skip - if dead then we should skip. if not we'll just get new req from gw
                                    }
                                }
                                if(jrfStarted){
                                    //when a follower becomes a leader it should shutdown its worker thread.
                                    jrfThread.interrupt();
                                }
                                rrlThread = Util.startAsDaemon(new RoundRobinLeader(this, peerIDtoAddress, servers, nextWorker, tcpServer), "RRL Thread");//Start the round robin leader thread
                                rrlStarted = true;//similar reason as before (jrfStarted)
                        }
                        break;
                    case OBSERVER:
                        if(currentLeader.getProposedLeaderID() == id) {//when the observer's leader is itself this tells us the observer needs to find the new leader (enter election) since it is impossible for an observe to be anything but an observer.
                            Thread.sleep(10000);//WAIT FOR EVERYONE TO GET IN ELECTION STATE SO WE DONT MISS ANY VOTES
                            ZooKeeperLeaderElection observe = new ZooKeeperLeaderElection(this, this.incomingMessages);
                            observe.lookForLeader();
                        }
                        break;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        outgoingMessages.offer(new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort()));
    }

    /*send an election message to all servers in the cluster*/
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            //log.fine("I am PS " + getServerId() + "  and I am sending my vote for " + currentLeader + " to PS # " + entry.getKey());
            outgoingMessages.offer(new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, entry.getValue().getHostString(), entry.getValue().getPort() ));
        }
    }

    public void addGossipMessageToList(GossipMessageForHTTP msg){
        this.gossipMessageList.add(msg);
    }

    public ServerState getPeerState() {
        return this.state;
    }

    public void setPeerState(ServerState newState) {
        if(!(this instanceof GatewayPeerServerImpl)) {
            log.info(id + ": switching from " + this.state + " to " + newState);
            System.out.println(id + ": switching from " + this.state + " to " + newState);
        }
        this.state = newState;
    }

    public Long getServerId() {
        return this.id;
    }

    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    public InetSocketAddress getAddress() {
        return myAddress;
    }

    public int getUdpPort() {
        return this.myPort;
    }

    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    public int getQuorumSize() {
        int qSize = (int) Math.ceil(((peerIDtoAddress.size() - 1)/2.0) + 1);//quorum size responds to dead peers. (-1 for Gateway Server)
        return qSize;

    }

    @Override
    public boolean isPeerDead(long peerID){
        //If the id were checking for life is our own than return false - we are trivially alive (so we are not dead)- also it will produce a null pointer exception in peerIdtoaddr
        if(peerID == id) {
            return false;
        }
        return peerIDtoAddress.get(peerID) == null;
    }
    @Override
    public boolean isPeerDead(InetSocketAddress addr){
        return  deadPeersList.get(addr) == -1;//-1 means it's ID has been removed and it is dead.
    }

    /*This is the method that is essential for the Round Robin logic for a leader server
    * it is synchronized since multiple Round Robin Leader Helper's will be
    * asynchronously(independently) handling client requests. They therefor need to coordinate
    * WHO exactly is the next worker. (I use atomic variable as well though I believe it may be unnecessary.)
    * */
    public synchronized InetSocketAddress getNextWorker(){
        if (nextWorker.get() >= servers.size()) { //once the nextWorker variable == to the amount of servers, then we have reached the end of our worker list and must start again
            nextWorker.set(1);//to avoid OBSERVER
        }
        return peerIDtoAddress.get(servers.get(nextWorker.getAndIncrement()));//use the nextWorker index to get the next worker from the servers list.
    }

    //maybe unnecessary
    public void setObserverID(InetSocketAddress addr){
        if(this.observerID == -1) {
            try {
                this.observerID = deadPeersList.get(addr);
            }catch(NullPointerException e){
               // System.out.println("NULL POINTER: " + addr);
            }
        }
    }

    /*THESE ARE METHODS FOR THREADS TO WORK WITH THE
    PENDING MESSAGES DATA STRUCTURE (REALLY ONLY THE LAST ONE IS NECESSARY)
     */
    public void addPendingMessage(Message msg){
        this.pendingMessages.add(msg);
    }
    public Message getPendingMessage(){
        if(!pendingMessages.isEmpty()) {
            return  this.pendingMessages.get(0);//queue style
        }
        return null;
    }
    public Message removeMessage(){
        if(!pendingMessages.isEmpty()) {
            return this.pendingMessages.remove(0);//queue style
        }
        return null;
    }

    public ArrayList<Message> getPendingMessages() {
        return pendingMessages;
    }
    /*END OF PENDINGMESSAGES METHODS*/

    public void incrPeerEpoch(){
        this.peerEpoch++;
    }

    /*Helper methods for testing purposes*/
    public boolean signaledToShutdown() {
        return signaledToShutdown;
    }

    public void signalToShutdown() {
        this.signaledToShutdown = true;
    }
    /*end of testing helper methods*/

    public void activateHTTPServices(){
        HttpServer server = null;
        try {
            server = HttpServer.create(myAddress, 500);//can we have two http servers at same addresss and port (relevant for gateway)
            server.createContext("/getserverstatus", new StatusUpdater());
            server.createContext("/GossipLogs", new GossipLogHandler());

            server.setExecutor(Executors.newCachedThreadPool()); // creates a default executor
            server.start();

        }catch(IOException e){
            e.printStackTrace();
        }
    }
    class StatusUpdater implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {//takes client request
                String str = (getCurrentLeader().getProposedLeaderID() + "=LEADING " + observerID + "=OBSERVER ");
                for (Long id : servers) {
                    if (id != getCurrentLeader().getProposedLeaderID() && id != observerID) {
                        str = str + id + "=FOLLOWING ";
                    }
                }
                byte[] codeResult = str.getBytes();
                t.sendResponseHeaders(200, codeResult.length);//FIXME CHANGED
                OutputStream os = t.getResponseBody();
                os.write(codeResult);//send back to client
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
                try {
                    gossipMessageLog = initializeLogging("Server-with-port-" + getUdpPort() + "-gossip-message-list" , false);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                StringBuilder gossipListString = new StringBuilder();
                gossipListString.append("This is the Gossip Log for server with ID " + id + " at port " + myPort + "\n");
                for(int i = 0; i < gossipMessageList.size(); i++){
                    gossipMessageLog.info(gossipMessageList.get(i).toString());
                    gossipListString.append(gossipMessageList.get(i).toString());
                }

                //signal to client youre done - client should now have acces to that file
                //how do we send list over http. ????


                byte[] result = gossipListString.toString().getBytes();
                t.sendResponseHeaders(200, result.length);//FIXME CHANGED
                OutputStream os = t.getResponseBody();
                os.write(result);//send back to client //FIXME CHANGED
            } catch (IOException | NullPointerException e) {
            }
        }

    }
   public static Map<Long, InetSocketAddress> convertWithStream(String mapAsString) {
       Map<Long, InetSocketAddress> map = Arrays.stream(mapAsString.split(","))
               .map(entry -> entry.split("="))
               .collect(Collectors.toMap(entry -> Long.parseLong(entry[0]), entry -> new InetSocketAddress(entry[1].substring(0,entry[1].indexOf(":")),Integer.parseInt(entry[1].substring(entry[1].indexOf(":") + 1)))));
       return map;
   }
    public Logger getGossipMessageLog() {
        return gossipMessageLog;
    }

    public ArrayList<GossipMessageForHTTP> getGossipMessageList() {
        return gossipMessageList;
    }

   public static void main(String[] args) {
       int myPort = Integer.parseInt(args[0]);
       long myID = Long.parseLong(args[1]);
       boolean isGateway = Boolean.parseBoolean(args[2]);
       String str = args[3];
       ConcurrentHashMap<Long, InetSocketAddress> map = new ConcurrentHashMap<>(convertWithStream(str));
       //take out yourself
       map.remove(myID);
       if(isGateway){
            GatewayServer gs = new GatewayServer(myPort,0,myID,map);//WATCH FOR THIS
            gs.start();
       }else{
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(myPort,0,myID,map);
            server.activateHTTPServices();
            Thread t = new Thread(server, "Server on port " + server.getAddress().getPort());//changed getMyAddress to getAddress
            t.start();
       }
   }
}
