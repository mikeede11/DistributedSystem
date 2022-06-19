package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;

public class GossipMessageForHTTP {
    private final String messageContents;
    private final InetSocketAddress senderAddress;
    private final long timeReceived;

    public GossipMessageForHTTP(String messageContents, InetSocketAddress senderID, long timeReceived) {
        this.messageContents = messageContents;
        this.senderAddress = senderID;
        this.timeReceived = timeReceived;
    }


    public String getMessageContents() {
        return messageContents;
    }

    public InetSocketAddress getSenderAddress() {
        return senderAddress;
    }

    public long getTimeReceived() {
        return timeReceived;
    }

    @Override
    public String toString(){
        return "\tMachine at Address: " + senderAddress + " sent this gossip data\n" + messageContents + "\n\t at time " + timeReceived + "\n";
    }
}
