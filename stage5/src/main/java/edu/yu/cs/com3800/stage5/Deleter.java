package edu.yu.cs.com3800.stage5;

import java.util.concurrent.ConcurrentHashMap;

public class Deleter implements Runnable {
    static final int GOSSIP = 1000;
    static final int FAIL = GOSSIP * 11;
    static final int CLEANUP = FAIL * 2;
    private ConcurrentHashMap<Long, Gossip> gossipData;
    private Long serverToDelete;

    public Deleter(ConcurrentHashMap<Long, Gossip> gossipData, Long serverToDelete) {
        this.gossipData = gossipData;
        this.serverToDelete = serverToDelete;
    }

    @Override
    public void run(){
        try {
            Thread.sleep(CLEANUP);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        gossipData.remove(serverToDelete);
        //System.out.println("PS with ID " + " deleted");
    }
}
