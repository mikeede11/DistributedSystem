
package edu.yu.cs.com3800;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UDPMessageSender extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> outgoingMessages;
    public Logger senderLogger;
    private int serverUdpPort;

    public UDPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, int serverUdpPort) {
        this.outgoingMessages = outgoingMessages;
        setDaemon(true);
        this.serverUdpPort = serverUdpPort;
        setName("UDPMessageSender-port-" + this.serverUdpPort);
    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                if (this.senderLogger == null) {
                    this.senderLogger = initializeLogging(this.getClass().getCanonicalName() + "-on-server-with-udpPort-" + this.serverUdpPort, true);
                }
                Message messageToSend = this.outgoingMessages.poll();
                if (messageToSend != null) {

                    DatagramSocket socket = new DatagramSocket();
                    byte[] payload = messageToSend.getNetworkPayload();
                    DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, new InetSocketAddress(messageToSend.getReceiverHost(), messageToSend.getReceiverPort()));
                    socket.send(sendPacket);
                    socket.close();

                    this.senderLogger.fine(serverUdpPort + " to " + messageToSend.getReceiverPort() + "Message sent: \n" + messageToSend.toString());
                }
            } catch (Exception e) {
                this.senderLogger.log(Level.WARNING, "failed to send packet", e);
            }
        }
    }
}