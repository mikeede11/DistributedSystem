package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {
    Logger gpsLog;
    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress) {
        super(myPort, peerEpoch, id, peerIDtoAddress);
        setPeerState(ServerState.OBSERVER);
        super.log.info(id + ": starting in " + ServerState.OBSERVER);
        System.out.println(id + ": starting in " + ServerState.OBSERVER);
        try {
            gpsLog = initializeLogging(GatewayPeerServerImpl.class.getCanonicalName() + "-with-port-" + super.getUdpPort() , false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
