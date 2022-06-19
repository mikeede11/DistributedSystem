
package edu.yu.cs.com3800;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;
    private ZooKeeperPeerServer myPeerServer;
    private LinkedBlockingQueue<Message> incomingMessages;
    private boolean electionDecided;
    HashMap<Long, ElectionNotification> voterIDtoVote = new HashMap<>();

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
    }

    private synchronized Vote getCurrentVote() {
        return this.myPeerServer.getCurrentLeader();

    }

    public synchronized Vote lookForLeader() {
        //send initial notifications to other peers to get things started
        if((this.myPeerServer.getPeerState() == LOOKING)) {
            voterIDtoVote.put(this.myPeerServer.getServerId(), new ElectionNotification(getCurrentVote().getProposedLeaderID(), myPeerServer.getPeerState(), myPeerServer.getServerId(), getCurrentVote().getPeerEpoch()));
            sendNotifications();
        }

        /*this vote will be used to hold on to a previous vote in the event that this vote is about to be declared election winner,
         but is "disregarded" because we notice other votes in the queue.
         So since those votes may actually mean this vote is NOT the election winner we drop this one to check those out.
         However if those new votes do not change anything then the previous vote should have been declared the winner,
         however since we only evaluate a vote for winner UPON RECEPTION that vote may never become the leader.
         To avoid this we "hold on" to that previous vote with this variable. So the one true king will lead. Long may he reign :)
         */
         Vote previousVote = null;

        //Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == LOOKING || !electionDecided) {
            //Remove next notification from queue, timing out after 2 times the termination time
            Message msg = null;
            try {
                msg = incomingMessages.poll(maxNotificationInterval * 2, TimeUnit.MILLISECONDS);

                //this if-else is technically not necessary for my implementation. since i use queues strictly for election messages,
                // but I left it since it makes the code able to share a queue with other messages types and thus more robust.
                if(msg != null && msg.getMessageType() == Message.MessageType.ELECTION){
                    //were good
                }
                else if(msg != null){
                    incomingMessages.offer(msg);
                    msg = null;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //if no notifications received..
            if (msg == null){
                //..resend notifications to prompt a reply from others..
                sendNotifications();
                //and implement exponential back-off when notifications not received..
            }
            else {
                //ok we have received an vote - lets process it!
                ElectionNotification vote = getNotificationFromMessage(msg);
                //System.out.println("I am PS " + myPeerServer.getUdpPort() + "with ID " + myPeerServer.getServerId() + " and I have recieved this vote" + vote);
                if (msg.getMessageType().equals(Message.MessageType.ELECTION) && vote.getPeerEpoch() >= getCurrentVote().getPeerEpoch()) {//first conditional is unnecessary.
                    //only consider the vote if it has a valid epoch

                    //switch on the state of the sender:
                    if(this.myPeerServer.getPeerState() == OBSERVER){
                        //this is basically the same logic as for all the other servers.
                        // The only difference is it leaves out sending and updating my votes to others (as it should by def. of observer).
                        observe(vote);

                    }
                    else {
                        switch (vote.getState()) {
                            case LOOKING:
                                //This part deals with updating my own vote based on received vote
                                //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                                if (supersedesCurrentVote(vote.getProposedLeaderID(), vote.getPeerEpoch())) {
                                    try {
                                        this.myPeerServer.setCurrentLeader(vote);
                                        voterIDtoVote.put(this.myPeerServer.getServerId(), vote);//update my vote in map
                                        sendNotifications();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }

                                //This part keeps track of the votes I received and who I received them from. Each time this gets us closer to declaring a winner.
                                voterIDtoVote.put(vote.getSenderID(), vote);


                                //This part checks if I have enough votes to declare this vote as the leader:
                                if (haveEnoughVotes(voterIDtoVote, vote)) {
                                    //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                                    try {
                                        Thread.sleep(4000);//wait a bit for votes. Count all the votes! (in truth determining this # is a bit of an art)
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    if(incomingMessages.size() > 0) {
                                            previousVote = vote;
                                            continue;
                                    }


                                    //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone else won the election) and exit the election
                                    acceptElectionWinner(vote);
                                }/////////////////////THIS AREA NEEDS TO BE EXAMINED/THOUROUGHLY TESTED
                                else if(previousVote != null && haveEnoughVotes(voterIDtoVote, vote)){//huh?
                                    try {
                                        Thread.sleep(4000);
                                    } catch (Exception e) {
                                    }
                                    if(incomingMessages.size() > 0) {
                                        previousVote = vote;
                                        continue;
                                    }
                                    acceptElectionWinner(vote);
                                }
                                //need to add the prevvote doublecheck
                                ////////////////////THIS AREA NEEDS TO BE EXAMINED/THOUROUGHLY TESTED
                                break;
                            case FOLLOWING:
                            case LEADING: //if the sender is following a leader already or thinks it is the leader
                                //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                                //if so, accept the election winner.
                                //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                                if (vote.getPeerEpoch() == getCurrentVote().getPeerEpoch() && haveEnoughVotes(voterIDtoVote, vote)) {
                                    acceptElectionWinner(vote);
                                } else if (vote.getPeerEpoch() > getCurrentVote().getPeerEpoch()) {
                                    if (haveEnoughVotes(voterIDtoVote, vote)) {
                                        try {
                                            synchronized (this) {
                                                this.myPeerServer.setCurrentLeader(vote);
                                            }
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                        acceptElectionWinner(vote);
                                    } else {
                                        continue;
                                    }
                                }
                                //ELSE: if n is from a LATER election epoch
                                //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                                //THEN accept their leader, and update my epoch to be their epoch
                                //ELSE:
                                //keep looping on the election loop.
                        }
                    }
                }
            }
        }

        return this.myPeerServer.getCurrentLeader();

    }

    /*Use this to receive a vote via UDP*/
    static ElectionNotification getNotificationFromMessage(Message message){
        ByteBuffer msgBytes = ByteBuffer.wrap(message.getMessageContents()); // big-endian by default
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();
        return new ElectionNotification(leader, getServerState(stateChar), senderID, peerEpoch);
    }

    /*Use this to send a vote over UDP*/
    static byte[] buildMsgContent(ElectionNotification vote){
        ByteBuffer msgBytes = ByteBuffer.allocate(26);//8 bytes for long leader, 2 bytes for char state, 8 for long senderID, and 8 for long peerEpoch
        msgBytes.putLong(vote.getProposedLeaderID());
        msgBytes.putChar(vote.getState().getChar());
        msgBytes.putLong(vote.getSenderID());
        msgBytes.putLong(vote.getPeerEpoch());
        return msgBytes.array();
    }

    private void sendNotifications(){
        this.myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(new ElectionNotification(getCurrentVote().getProposedLeaderID(), this.myPeerServer.getPeerState(), this.myPeerServer.getServerId(),getCurrentVote().getPeerEpoch())));
    }

    private Vote acceptElectionWinner(ElectionNotification candidate) {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        if(this.myPeerServer.getPeerState() != OBSERVER) {
            if (candidate.getProposedLeaderID() == this.myPeerServer.getServerId()) {
                this.myPeerServer.setPeerState(LEADING);
                //                System.out.println("LEADER WAS SET!!!!");
            } else {
                this.myPeerServer.setPeerState(FOLLOWING);
                //                System.out.println("FOLLOWER WAS SET!!!!");
            }
        }
        else {
            try {
//                System.out.println("Observer found Leader!");
                this.myPeerServer.setCurrentLeader(candidate);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        electionDecided = true;//this may be unnecessary
        incomingMessages.clear();//this good or bad?

        return candidate;//the vote that won
    }
    private void observe(ElectionNotification vote){
        switch (vote.getState()) {
            case LOOKING: //if the sender is also looking
                //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                voterIDtoVote.put(vote.getSenderID(), vote);
                if (haveEnoughVotes(voterIDtoVote, vote)) {
                    //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                    try {
                        Thread.sleep(4000);
                    } catch (Exception e) {
                    }
                    if(incomingMessages.size() > 0) {
                        return;//continue
                    }
                    //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone lese won the election) and exit the election
                    acceptElectionWinner(vote);
                }
                break;
            case FOLLOWING:
            case LEADING: //if the sender is following a leader already or thinks it is the leader
                //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                //if so, accept the election winner.
                //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                if (vote.getPeerEpoch() == getCurrentVote().getPeerEpoch() && haveEnoughVotes(voterIDtoVote, vote)) {
                    acceptElectionWinner(vote);
                } else if (vote.getPeerEpoch() > getCurrentVote().getPeerEpoch()) {
                    if (haveEnoughVotes(voterIDtoVote, vote)) {
                        try {
                            synchronized (this) {
                                this.myPeerServer.setCurrentLeader(vote);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        acceptElectionWinner(vote);
                    }
                }
        }
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > getCurrentVote().getPeerEpoch()) || ((newEpoch == getCurrentVote().getPeerEpoch()) && (newId > getCurrentVote().getProposedLeaderID()));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        int numOfVotesForProp = 0;
        //Tally up the proposal's votes
        for (Map.Entry<Long, ElectionNotification>  entry : votes.entrySet()) {
            if(entry.getValue().getProposedLeaderID() == proposal.getProposedLeaderID()){
                numOfVotesForProp++;
            }
        }
//        System.out.println(numOfVotesForProp  + " >= " + this.myPeerServer.getQuorumSize());
        return numOfVotesForProp >= this.myPeerServer.getQuorumSize();
    }
}