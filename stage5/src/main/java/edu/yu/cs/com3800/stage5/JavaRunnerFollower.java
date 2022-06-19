package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {//TODO IS THIS OKAY DOES THIS CLASS SIGNATURE?

    private ZooKeeperPeerServerImpl server;
    private ServerSocket tcpServer;
    private Logger workerLog;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, ServerSocket tcpServer){
        this.server = server;
        this.tcpServer  = tcpServer;
    }

    @Override
    public void run() {
        try {
            workerLog = initializeLogging(this.getClass().getCanonicalName() + "-with-port-" + server.getUdpPort() , false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while(!Thread.currentThread().isInterrupted() && (server.getPeerState() == ZooKeeperPeerServer.ServerState.FOLLOWING || server.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING)) {//these conditionals are basically equivalent. A JRF should be running if its following (duh) or looking so that when a leader is found it will work with that guy. The only case this thread will stop is if this Server assumes Leader, in which case this thread will be interrupted via the logic under LEADING in ZKPSI. The only reason I have the thread interrupted test is because it is good practice and provides a more general way to shutdown the thread.
            if(server.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING){
                try {
                    Thread.sleep(2000);//wait until this server finishes election before accepting TCP connections (it does not know who it is supposed to connect to yet!)
                } catch (InterruptedException e) {
                   // e.printStackTrace();
                }
                continue;//re-check condition
            }
            try {
                tcpServer.setSoTimeout(10000);//every 10 seconds check if leader still alive otherwise we need an election
                Socket master = tcpServer.accept();

                InputStream streamFromMaster = master.getInputStream();
                JavaRunner jr = new JavaRunner();
                if(Util.bytesReadableAndPeerisAlive(streamFromMaster, server, server.getPeerByID(server.getCurrentLeader().getProposedLeaderID()))) {//true when worker doing work for dead leader?
                    byte[] messageFromMaster = Util.readAllBytes(streamFromMaster);
                    Message msgFromMaster = new Message(messageFromMaster);
                    workerLog.info("Received this from master" + new String(msgFromMaster.getMessageContents()));
                    //System.out.println("Received this from master" + new String(msgFromMaster.getMessageContents()));
                    Message msgToLeader = null;
                    if(msgFromMaster.getMessageType() == Message.MessageType.WORK) {
                        InputStream code = new ByteArrayInputStream(msgFromMaster.getMessageContents());
                        //System.out.println("About to do the work");
                        String result = jr.compileAndRun(code);
                        workerLog.info("Work Completed!");
                        //System.out.println("Work Completed!");
                        msgToLeader = new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(), msgFromMaster.getReceiverHost(), msgFromMaster.getReceiverPort(), msgFromMaster.getSenderHost(), msgFromMaster.getSenderPort(), msgFromMaster.getRequestID());//FIXME CHANGED
                    }else if(msgFromMaster.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK){
                        msgToLeader = server.getPendingMessage();
                        if(msgToLeader == null){
                            msgToLeader = new Message(Message.MessageType.COMPLETED_WORK, new byte[]{}, "",-1,"", -1, -1);
                        }
                        else{
                            msgToLeader = new Message(Message.MessageType.COMPLETED_WORK, msgToLeader.getMessageContents(), msgFromMaster.getReceiverHost(), msgFromMaster.getReceiverPort(),  msgFromMaster.getSenderHost(), msgFromMaster.getSenderPort(), msgToLeader.getRequestID());
                            workerLog.info("Sending Old Message To new Leader");
                        }

                    }

                    //for testing purposes
                      if(this.server.signaledToShutdown()){
                          workerLog.info("Shutdown");
                          //System.out.println("Follower with ID " + server.getServerId() + " shutdown");
                          server.shutdown();
                    }

                      //follower needs to send a did message if it doesnt have anything from last time or beg
                    //and leader needs to ignore it once received
                    if(msgToLeader.getRequestID() == -1 || !server.isPeerDead(new InetSocketAddress(msgToLeader.getReceiverHost(),msgToLeader.getReceiverPort()))) {
                        OutputStream streamToLeader = master.getOutputStream();
                        streamToLeader.write(msgToLeader.getNetworkPayload());
                        workerLog.info("message sent");
                        //System.out.println("Message sent to leader");
                        Message tempMsg = server.removeMessage();
                        server.addPendingMessage(msgToLeader);
                    }else if(server.getPeerState() != ZooKeeperPeerServer.ServerState.LOOKING){
                        this.server.setCurrentLeader(new Vote(server.getServerId(), server.getPeerEpoch() + 1));
                        this.server.incrPeerEpoch();
                        this.server.setPeerState(ZooKeeperPeerServer.ServerState.LOOKING);
                    }
                }
            }catch (Exception e){
//                System.out.println("Follower prob timed out");
                    if(!server.isPeerDead(server.getCurrentLeader().getProposedLeaderID())){
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            //ex.printStackTrace();
                        }
                    }

                if(server.isPeerDead(server.getCurrentLeader().getProposedLeaderID()) && server.getPeerState() != ZooKeeperPeerServer.ServerState.LOOKING){
                    //System.out.println("follower noticed leader dead");
                    try {
                        this.server.setCurrentLeader(new Vote(server.getServerId(), server.getPeerEpoch() + 1));
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    this.server.incrPeerEpoch();
                    this.server.setPeerState(ZooKeeperPeerServer.ServerState.LOOKING);//no msg yet
                }
            }
        }
    }
}
