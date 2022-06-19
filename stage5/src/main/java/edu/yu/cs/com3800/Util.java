package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.Gossip;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Util {

    public static byte[] readAllBytesFromNetwork(InputStream in)  {
        try {
            while (in.available() == 0) {
                try {
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                }
            }
        }
        catch(IOException e){}
        return readAllBytes(in);
    }

    public static boolean bytesReadableAndPeerisAlive(InputStream in, ZooKeeperPeerServerImpl server, InetSocketAddress addrToWorker)  {
        try {
            while (in.available() == 0 && !server.isPeerDead(addrToWorker)) {
                try {
                    Thread.currentThread().sleep(500);
                    //System.out.println("I am PS " + server.getServerId() + "and I am waiting for a response from " + addrToWorker);
                }
                catch (InterruptedException e) {
                }
            }
            return !server.isPeerDead(addrToWorker);//If we got past that loop it means either theres something to read or the peer we are waiting for is dead so we should move on
        }
        catch(IOException e){return false;}
    }

    public static byte[] readAllBytes(InputStream in) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException e){
            System.out.println("EXCEPTION OCCURED");
        }
        return buffer.toByteArray();
    }

    public static byte[] gossipToBytes(ConcurrentHashMap<Long, Gossip> map){
        int bufSize = Long.BYTES * 3 * map.size();
        ByteBuffer buffer = ByteBuffer.allocate(bufSize);
        buffer.clear();
        for(Map.Entry<Long, Gossip> entry: map.entrySet()){
            buffer.putLong(entry.getKey());
            buffer.putLong(entry.getValue().getHeartbeatcounter());
            buffer.putLong(entry.getValue().getTimeOFLastUpdate());

        }
        byte[] result = buffer.array();
        return  result;
    }

    //this method is a little to expensive for my taste
    public static ConcurrentHashMap<Long, Gossip> bytesToGossip(byte[] bytes){
        ConcurrentHashMap<Long, Gossip> gossipMap = new ConcurrentHashMap<>();
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.clear();
        int elements = bytes.length /(Long.BYTES * 3);//each item has 3 longs ID, counter, and time
        for(int i = 0; i < elements; i++){
            gossipMap.put(buf.getLong(), new Gossip(buf.getLong(), buf.getLong()));
        }
        return gossipMap;
    }

    public static Thread startAsDaemon(Thread run, String name) {
        Thread thread = new Thread(run, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    public static String getStackTrace(Exception e){
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        PrintStream myErr = new PrintStream(bas,true);
        e.printStackTrace(myErr);
        myErr.flush();
        myErr.close();
        return bas.toString();
    }
}