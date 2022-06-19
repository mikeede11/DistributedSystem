package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class Stethoscope extends Thread{
    static final int GOSSIP = 1000;
    static final int FAIL = GOSSIP * 11;
    static final int CLEANUP = FAIL * 2;

    private LinkedBlockingQueue<Message> gossipIn;
    private ConcurrentHashMap<Long,Gossip> peerIDToGossipData;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<Long> servers;
    private boolean exit;
    private ConcurrentHashMap<InetSocketAddress, Long> deadPeersList;
    private ZooKeeperPeerServerImpl server;
    private Logger log;

    //the gossip table, ID to Address map, server list, and dead peers list are all passed in to update when peer is discovered dead.
    public Stethoscope(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, ConcurrentHashMap<Long, Gossip> peerIDToGossipData, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, ArrayList<Long> servers, ConcurrentHashMap<InetSocketAddress, Long>  deadPeersList, Logger log) {
        this.gossipIn = incomingMessages;
        this.peerIDToGossipData = peerIDToGossipData;
        this.peerIDtoAddress = peerIDtoAddress;
        this.servers = servers;
        this.exit = false;
        this.deadPeersList = deadPeersList;
        this.server = server;
        this.log = log;
    }

    @Override
    public void run() {
        ConcurrentHashMap<Long,Gossip> tempMap = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newCachedThreadPool();

        while(!exit){
            Message m = null;
            try {
                m = gossipIn.poll(GOSSIP, TimeUnit.MILLISECONDS);//wait for gossip
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(m == null){
                continue;
            }
            tempMap = Util.bytesToGossip(m.getMessageContents());
            String mapString = "\t" + tempMap.toString().replaceAll(",", "\n\t");
            server.addGossipMessageToList(new GossipMessageForHTTP(mapString, new InetSocketAddress(m.getSenderHost(),m.getSenderPort()),System.currentTimeMillis()));//note: we can be assured that this sender is still alive and will therefore return an appropriate ID #. (remember if dead the value is -1 which isn't the ID number of the server - it just informs us that it is dead)
            //log.info("I am server with ID  " +  +" and I Received a Gossip Message from PS " + m.getSenderPort() + " with a heartbeat of ");// + tempMap.get(0).getHeartbeatcounter() + " for 0" + tempMap.get(1).getHeartbeatcounter() + " for 1" + tempMap.get(2).getHeartbeatcounter() + " for 2" + tempMap.get(3).getHeartbeatcounter()+ " for  AT TIME " + System.currentTimeMillis());
            for(Map.Entry<Long, Gossip> entry: tempMap.entrySet()) {//update entry and then check if "dead"
                if (!(server.isPeerDead(entry.getKey())) && entry.getValue().getHeartbeatcounter() > peerIDToGossipData.get(entry.getKey()).getHeartbeatcounter()) {
                    peerIDToGossipData.get(entry.getKey()).setHeartbeatcounter(tempMap.get(entry.getKey()).getHeartbeatcounter());
                    long time = System.currentTimeMillis();
                    peerIDToGossipData.get(entry.getKey()).setTimeOFLastUpdate(time);
                    log.info(server.getServerId() + ": updated " + entry.getKey() + "’s heartbeat sequence to " + entry.getValue().getHeartbeatcounter() + " based on message from " + deadPeersList.get(new InetSocketAddress(m.getSenderHost(), m.getSenderPort())) + " at node time " + time);
                } else if (!(server.isPeerDead(entry.getKey())) && (System.currentTimeMillis() - peerIDToGossipData.get(entry.getKey()).getTimeOFLastUpdate()) > FAIL) {
                    //Logic: ok so this peer server is considered dead now, if we didnt already remove him do as follows
                    //A) put him on deadPeersList (primarily for ispeerdead(address))
                    //B) execute a task that will delete this PS from gossip table in CLEANUP time
                    //C) actually remove him from DS so we dont do all this stuff again (CLEANUP is a long time and we will most likely come back around)
                    //   and so we dont send work, votes or gossip messages. this may cause problems or null pointer exceptions if not careful
                    if (peerIDtoAddress.get(entry.getKey()) != null) {//CAN GET RID OF SUPERFLUOUS
                        log.info(server.getServerId() + ": no heartbeat from server " + entry.getKey() + "  server failed");
                        System.out.println(server.getServerId() + ": no heartbeat from server " + entry.getKey() + "  server failed");
                        deadPeersList.put(peerIDtoAddress.get(entry.getKey()), (long) -1);//what about + 4 addr?
                        deadPeersList.put(new InetSocketAddress(peerIDtoAddress.get(entry.getKey()).getHostString(), peerIDtoAddress.get(entry.getKey()).getPort() + 4), (long) -1);
                        deadPeersList.put(new InetSocketAddress(peerIDtoAddress.get(entry.getKey()).getHostString(), peerIDtoAddress.get(entry.getKey()).getPort() + 2), (long) -1);
                        executor.execute(new Deleter(peerIDToGossipData, entry.getKey()));
                        peerIDtoAddress.remove(entry.getKey());
                        servers.remove(entry.getKey());
                        //if we wait in this thread than all servers will die!
                    }
                }
            }
        }
    }

    public void shutdownStethoscope(){
        this.exit = true;
    }
}
