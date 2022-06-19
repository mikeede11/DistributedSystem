package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Stage4Demo {

    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

//    private LinkedBlockingQueue<Message> outgoingMessages;
//    private LinkedBlockingQueue<Message> incomingMessages;
private final HttpClient httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build();
   //private int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
   private  int[] ports = {8010, 8020, 8030, 8040, 8050};
    private int leaderPort = this.ports[this.ports.length - 1];
    private  int gatewayPort = ports[0];//1st element is gateway
    private int myPort = 9999;
    private Object[] responses;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ArrayList<ZooKeeperPeerServer> servers;
    private ArrayList<Thread> threads;

    public Stage4Demo() throws Exception {
        /*step 1: create sender & sending queue
        this.outgoingMessages = new LinkedBlockingQueue<>();
        UDPMessageSender sender = new UDPMessageSender(this.outgoingMessages, -1);//-1 last arg
         */
        //revised Create HTTP Client
        //step 2: create servers
        createServers();
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
        }
        printLeaders();/*
        try {
            Thread.sleep(30000);
        } catch (Exception e) {
        }
        printLeaders();*/
        /*//step 3: since we know who will win the election, send requests to the leader, this.leaderPort
        for (int i = 0; i < this.ports.length; i++) {
            String code = this.validClass.replace("world!", "world! from code version " + i);
            sendMessage(code);
        }
        Util.startAsDaemon(sender, "Sender thread");
        this.incomingMessages = new LinkedBlockingQueue<>();
        UDPMessageReceiver receiver = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, null);//null last arg
        Util.startAsDaemon(receiver, "Receiver thread");*/
        //Revised step 3 - send http requests to the GATEWAY that we created and we have gatewayPort
       responses = new Object[this.ports.length];
        for (int i = 0; i < this.ports.length; i++) {
            String code = this.validClass.replace("world!", "world! from code version " + i);
            HttpRequest request = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(code))
                    .uri(URI.create("http://localhost:" + gatewayPort + "/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .build();
            HttpResponse<String> response = null;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                responses[i] = response;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //step 4: validate responses from leader
        System.out.println("POINT BLANK");
        printResponses();
        System.out.println("POINT BLANK");
        //step 5: stop servers
        stopServers();
    }

    private void printLeaders() {
        System.out.println("IS THIS WORKING?");
        for (ZooKeeperPeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());//changed getAddress and getId
            }
        }
    }

    private void stopServers() {
        for (ZooKeeperPeerServer server : this.servers) {
            server.shutdown();
        }
    }

    private void printResponses() throws Exception {
        String completeResponse = "";
        for (int i = 0; i < this.ports.length; i++) {
            completeResponse += "Response #" + i + ":\n" + ((HttpResponse<String>)responses[i]).body() + "\n";
        }
        System.out.println(completeResponse);
    }

 /*   private void sendMessage(String code) throws InterruptedException {
        Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", this.leaderPort);
        this.outgoingMessages.put(msg);
    }*/

    private void createServers() {
        //create IDs and addresses
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(9);
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }
        //create servers
        this.servers = new ArrayList<>(4);
        this.threads = new ArrayList<>(4);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            ConcurrentHashMap<Long, InetSocketAddress> map = new ConcurrentHashMap<>(peerIDtoAddress);
            map.remove(entry.getKey());//removing itself
            if(entry.getValue().getPort() == gatewayPort) {
                GatewayServer gs = new GatewayServer(gatewayPort,0, entry.getKey(), map);//WOW USED PEERIDTOADDR INSTEAD OF MAP
                gs.start();
                servers.add(gs.getGwpsi());
            }
            else {
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
                this.servers.add(server);
                Thread t = new Thread(server, "Server on port " + server.getAddress().getPort());//changed getMyAddress to getAddress
                t.start();
                this.threads.add(t);
            }
        }
        for (Thread server: threads) {
            System.out.println(server.getName());
        }

    }

    public static void main(String[] args) throws Exception {
        new Stage4Demo();
       /* String code = validClass.replace("world!", "world! from code version ");

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .uri(URI.create("http://localhost:" + gatewayPort + "/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .build();
        HttpResponse<String> response = null;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("Response # " + args[0] + " is "+ response);*/
    }
}
