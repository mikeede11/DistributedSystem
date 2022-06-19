package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.ZooKeeperPeerServer;
import org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MinimumServersTester {
    private static String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();
    private static int[] ports = {8010, 8020, 8030, 8040, 8050};
    private static int leaderPort = ports[ports.length - 1];//highest port
    private static int gatewayPort = ports[0];//1st element is gateway
    private static int myPort = 9999;
    private static Object[] responses;
    private static InetSocketAddress myAddress = new InetSocketAddress("localhost", myPort);
    private static ArrayList<ZooKeeperPeerServer> servers;
    private static ArrayList<Thread> threads;

    @BeforeClass
    public static void setUp(){
        createServers();
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testAnoFaultSingleClientReqTest(){

        HttpRequest request = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(validClass))
                    .uri(URI.create("http://localhost:" + gatewayPort + "/compileandrun"))
                    .header("Content-Type", "text/x-java-source")
                    .build();
            HttpResponse<String> response = null;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                assertEquals("Hello world!", response.body());
            } catch (InterruptedException | IOException e) {
                fail();
                e.printStackTrace();
            }
    }

    @Test
    public void testBfollowerDiedBeforeTaskArrivedTest(){

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(validClass))
                .uri(URI.create("http://localhost:" + gatewayPort + "/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .build();
        HttpResponse<String> response = null;
        try {
            for(ZooKeeperPeerServer server: servers){
                if(server.getServerId() == 2){//
                    //this will be the follower that the SECOND (second test) client request
                    // will be sent to from the leader - as per round robin distribution.
                    server.shutdown();
                }
            }
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals("Hello world!", response.body());
        } catch (InterruptedException | IOException e) {
            fail();
            e.printStackTrace();
        }
    }

    /*This test actually tests alot. Primarily it tests if our system can recover when a follower died and was
      was sent the work already. However this additionally tests if the leader can recognize that a request was not received even though
      the leader sent it out. It also shows the leader can respond to that situation and not lose any client requests.
      It also tests some other things as described in the method.
     */
    @Test
    public void testCfollowerDiedAfterTaskArrivedTest(){

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(validClass))
                .uri(URI.create("http://localhost:" + gatewayPort + "/compileandrun"))
                .header("Content-Type", "text/x-java-source")
                .build();
        HttpResponse<String> response = null;
        try {
            for(ZooKeeperPeerServer server: servers){
                if(server.getServerId() == 1){//
                    //this will be the follower that the Third (third test) client request
                    // will be sent to from the leader - as per round robin distribution.
                    //first was sent to one, second was sent to 3 since 2 died before and
                    // now it will circle around again to 1. This should result in 3 receiving the work.
                    // this also tests if our system can handle two consecutive followers being unable to process
                    //a request
                    ((ZooKeeperPeerServerImpl)server).signalToShutdown();//a method designed to shut down a server in the middle of its activities
                }
            }
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals("Hello world!", response.body());
        } catch (InterruptedException | IOException e) {
            fail();
            e.printStackTrace();
        }
    }



    private static void createServers() {
        //create IDs and addresses
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(9);
        for (int i = 0; i < ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", ports[i]));
        }
        //create servers
        servers = new ArrayList<>(4);
        threads = new ArrayList<>(4);
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
                servers.add(server);
                Thread t = new Thread(server, "Server on port " + server.getAddress().getPort());//changed getMyAddress to getAddress
                t.start();
                threads.add(t);
            }
        }
        for (Thread server: threads) {
            System.out.println(server.getName());
        }

    }
}
