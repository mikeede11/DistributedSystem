package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class RRLHelper implements Runnable, LoggingServer {
    private Socket clientRequest;
    private Logger rrlHelperLog;
    private ZooKeeperPeerServerImpl server;
    private boolean messageSentToGateway;
    private boolean justElected;

    public RRLHelper(ZooKeeperPeerServerImpl server, Socket clientRequest, boolean justElected) {
        this.clientRequest = clientRequest;
        try {
            rrlHelperLog = initializeLogging(this.getClass().getCanonicalName() + "-using-a-worker" , false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.server = server;
        this.messageSentToGateway = false;
        this.justElected = justElected;
    }

    @Override
    public void run() {
        InputStream ois = null;
        OutputStream oos = null;
        try {
            ois = clientRequest.getInputStream();//receive from gateway
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] msgData = Util.readAllBytes(ois);
        Message msgFromGateway = new Message(msgData);

        server.setObserverID(new InetSocketAddress(msgFromGateway.getSenderHost(), msgFromGateway.getSenderPort() - 2));//MAY BE UNNECESSAry

        rrlHelperLog.info("This is what the the leader received from the gateway " + new String(msgFromGateway.getMessageContents()));
        //System.out.println("This is what the the leader received from the gateway " + new String(msgFromGateway.getMessageContents()));
        /* Implementation note - this is the leader responding to gateway requests for old work - like when a leader dies but the gateway didnt get the work back
        this needs to account for when this new leader was originally a follower. It could be that the used to be follower had pending messages (completed work).
        in which case we have to take that message and adjust its addresses - that is really all we'll need to do b/c the gateway will request it and it will have the correct reqID and will
         be eventually picked up like all the other old messages*/
        if(justElected){
            rrlHelperLog.info("waiting 10 seconds for other servers to finish election");
            //System.out.println("RRL Helper waiting 10 seconds for other servers to finish election");
            try {
                Thread.sleep(10000);//wait so all followers can finish election
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }
        }
        ArrayList<Message> refToList = server.getPendingMessages();
        /* Upon reception of a client request from gateway
        * first check if the work was already done (If it is in
        * our pendingMessages list. Use request IDs to identify.
        */
        for(int i = 0; i < refToList.size(); i++) {
            if (msgFromGateway.getRequestID() == refToList.get(i).getRequestID()) {
                Message msgToGateway = new Message(refToList.get(i).getMessageType(), refToList.get(i).getMessageContents(), msgFromGateway.getReceiverHost(), msgFromGateway.getReceiverPort(), msgFromGateway.getSenderHost(), msgFromGateway.getSenderPort(), refToList.get(i).getRequestID());//FIXME NO + 2 ALREADY ACCOUNTED FOR
                try {
                    oos = clientRequest.getOutputStream();//to send back to gateway
                    oos.write(msgToGateway.getNetworkPayload());
                    rrlHelperLog.info("Sent completed work back to gateway that was delayed by leader death");
                    //System.out.println("Sent completed work back to gateway that was delayed by leader death");
                    refToList.remove(i);
                    messageSentToGateway = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Socket workerSocket = null;
        //ROUND ROBIN LEADER Work distribution needs to be coordinated by threads since they are concurrently sending out work requests. this is achieved with the getNextWorker().
        for (InetSocketAddress addrToWorker = server.getNextWorker(); !messageSentToGateway && !Thread.currentThread().isInterrupted(); addrToWorker = server.getNextWorker()) {
            Message msgToWorker = new Message(msgFromGateway.getMessageType(), msgFromGateway.getMessageContents(), msgFromGateway.getReceiverHost(), msgFromGateway.getReceiverPort(), addrToWorker.getHostString(), addrToWorker.getPort() + 2, msgFromGateway.getRequestID());
            try {
                workerSocket = new Socket(addrToWorker.getHostString(), addrToWorker.getPort() + 2);// + 2 TCP port
                OutputStream streamToWorker = workerSocket.getOutputStream();
                streamToWorker.write(msgToWorker.getNetworkPayload());
                rrlHelperLog.info("Leader sent this:\n" + new String(msgToWorker.getMessageContents()) + "\nTo worker at Address "  + addrToWorker);
                //System.out.println("Leader sent this:\n" + new String(msgToWorker.getMessageContents()) + "\nTo worker at Address "  + addrToWorker);
                if(this.server.signaledToShutdown()){
                    //System.out.println("SHUTDOWN of RRLH INITIATED");
                    server.shutdown();
                }
                InputStream streamFromWorker = workerSocket.getInputStream();
                if (Util.bytesReadableAndPeerisAlive(streamFromWorker, server, addrToWorker)) {
                    byte[] result = Util.readAllBytes(streamFromWorker);
                    Message msgFromWorker = new Message(result);
                    //System.out.println("Received message from worker at address " + addrToWorker);
                    Message msgToGateway = new Message(msgFromWorker.getMessageType(), msgFromWorker.getMessageContents(), msgFromGateway.getReceiverHost(), msgFromGateway.getReceiverPort(), msgFromGateway.getSenderHost(), msgFromGateway.getSenderPort(), msgFromWorker.getRequestID());
                    oos = clientRequest.getOutputStream();//to send back to gateway
                    oos.write(msgToGateway.getNetworkPayload());
                    rrlHelperLog.info("sent result to gateway");
                    //System.out.println("sent result to gateway");
                    messageSentToGateway = true;//we are done - this thread has accomplished its task. It can terminate.
                    workerSocket.close();
                }
                //If the above if statement block was not executed
                //then the peer must be dead so were going to try
                // again with a new worker - do this until we send the result back to gateway
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}


