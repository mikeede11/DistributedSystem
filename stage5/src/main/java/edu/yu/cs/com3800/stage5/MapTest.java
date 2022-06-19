package edu.yu.cs.com3800.stage5;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class MapTest {
    public static void main(String[] args) {
        /*ConcurrentHashMap<Long, Gossip> map = new ConcurrentHashMap<>();
        map.put((long) 0, new Gossip(0, 613));

        map.put((long) 1, new Gossip(1, 100000));

        map.put((long) 2, new Gossip(2, 200000));

        map.put((long) 3, new Gossip(3, 300000));
        System.out.println(map);
        System.out.println(map.toString());*/

        int[] arr = new int[10];
        Long zerol = new Long(0);
        Long zero = 0L;
        long zerop = 0;
        System.out.println(zerol == zero );
        System.out.println(zerol.equals(zero) );
        System.out.println(zerol == zerop );
        System.out.println(zerol.equals(zerop) );
        System.out.println(zero == zerop);
        System.out.println(zero.equals(zerop));


    }
}
