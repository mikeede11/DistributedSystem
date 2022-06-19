package edu.yu.cs.com3800.stage5;

public class Gossip{
    private long heartbeatcounter;
    private long timeOFLastUpdate;

    //note: synchronization may be unnecessary
    public Gossip(long heartbeatcounter, long timeOFLastUpdate) {
        this.heartbeatcounter = heartbeatcounter;
        this.timeOFLastUpdate = timeOFLastUpdate;
    }

    public synchronized long getHeartbeatcounter() {
        return heartbeatcounter;
    }

    public synchronized void setHeartbeatcounter(long heartbeatcounter) {
        this.heartbeatcounter = heartbeatcounter;
    }

    public synchronized long incrHeartbeat() {
       return this.heartbeatcounter++;
    }

    public synchronized long getTimeOFLastUpdate() {
        return timeOFLastUpdate;
    }

    public void setTimeOFLastUpdate(long timeOFLastUpdate) {
        this.timeOFLastUpdate = timeOFLastUpdate;
    }

    @Override
    public String toString(){
        return "[Heartbeat: " + heartbeatcounter + " | Time of last update: " + timeOFLastUpdate +"]";
    }
}
