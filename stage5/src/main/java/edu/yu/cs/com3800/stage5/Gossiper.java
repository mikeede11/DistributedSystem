package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import java.net.InetSocketAddress;
import java.util.Random;

import java.util.concurrent.*;

public class Gossiper extends Thread {
    static final int GOSSIP = 1000;//send gossip 3 times quicker than what we consider a fail time. I felt this would decrease the chances of finding a failed server that did not actually fail.
    static final int FAIL = GOSSIP * 6;
    static final int CLEANUP = FAIL * 2;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private InetSocketAddress[] addresses;
    private Random rndm;
    private ZooKeeperPeerServer server;
    private ConcurrentHashMap<Long, Gossip> peerIDToGossipData;
    private boolean exit;

    //this server's list of other servers list
    //list of alive servers so we know whos elgible to send messages and to receive.

    public Gossiper(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> outgoingMessages, InetSocketAddress[] addresses, ConcurrentHashMap<Long, Gossip> peerIDToGossipData){
        this.outgoingMessages = outgoingMessages;
        this.addresses = addresses;
        this.server = server;
        this.peerIDToGossipData = peerIDToGossipData;
        rndm = new Random();
        this.exit = false;
    }
    @Override
    public void run() {
        ConcurrentHashMap<Long, Gossip> tempMap = null;
        ExecutorService executor = Executors.newCachedThreadPool();
        int randomRecipient = 0;
        while(!exit){
            try {
                Thread.sleep(GOSSIP);//send out every GOSSIP seconds
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            peerIDToGossipData.get(this.server.getServerId() ).incrHeartbeat();//increment heartbeat
            peerIDToGossipData.get(this.server.getServerId() ).setTimeOFLastUpdate(System.currentTimeMillis());//timestamp
            randomRecipient = rndm.nextInt(addresses.length );//send to random server.
            //System.out.println("I AM PS ID " + this.server.getServerId() + " and I am sending a heart beat (" +  peerIDToGossipData.get(server.getServerId()).getHeartbeatcounter() + " with a last update of " + peerIDToGossipData.get(server.getServerId()).getTimeOFLastUpdate() + ") to PS at port " + addresses[randomRecipient].getPort());
            outgoingMessages.offer(new Message(Message.MessageType.GOSSIP, Util.gossipToBytes(peerIDToGossipData), server.getAddress().getHostString(), server.getUdpPort() + 4, addresses[randomRecipient].getHostString(),  addresses[randomRecipient].getPort() + 4));

        }
    }

    public void shutdownGossiper(){
        this.exit = true;
    }
}
