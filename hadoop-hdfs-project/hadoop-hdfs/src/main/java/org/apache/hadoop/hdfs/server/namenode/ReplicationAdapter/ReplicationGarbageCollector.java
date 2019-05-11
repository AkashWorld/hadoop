package org.apache.hadoop.hdfs.server.namenode.ReplicationAdapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicReference;

class ReplicationGarbageCollector {
    public static final Log auditLog = LogFactory.getLog(
            ReplicationGarbageCollector.class.getName() + ".audit");
    private class LRUList{
        class Data implements Comparable<Data>{
            String fileName;
            long time;
            short replication;
            public int compareTo(Data d){
                return (int) (d.time - this.time);
            }
        }
        PriorityQueue<Data> pqueue;
        Map<String, Data> lookupTable;
        LRUList(){
            pqueue = new PriorityQueue<Data>();
            lookupTable = new HashMap<String, Data>();
        }
        void touchFile(String filename, HdfsFileStatus stat){
            if(this.lookupTable.containsKey(filename)){
                Data data = this.lookupTable.get(filename);
                pqueue.remove(data);
                data.time = System.currentTimeMillis();
                data.replication = stat.getReplication();
                pqueue.add(data);
            }
            else {
                Data data = new Data();
                data.fileName = filename;
                data.time = System.currentTimeMillis();
                lookupTable.put(filename, data);
                pqueue.add(data);
            }
        }
        void touchFile(String filename, FileStatus stat){
            if(this.lookupTable.containsKey(filename)){
                Data data = this.lookupTable.get(filename);
                pqueue.remove(data);
                data.time = System.currentTimeMillis();
                data.replication = stat.getReplication();
                pqueue.add(data);
            }
            else {
                Data data = new Data();
                data.fileName = filename;
                data.time = System.currentTimeMillis();
                lookupTable.put(filename, data);
                pqueue.add(data);
            }
        }
    }
    private final Configuration conf;
    private final FSNamesystem fsns;
    private AtomicReference<LRUList> atomicLRU;
    private int DEFAULT_REPLICATION = 3;
    long GC_INTERVAL = 2500;
    CleanUpThread cleanUpThread;
    ReplicationGarbageCollector(Configuration conf, FSNamesystem fsns){
        this.conf = conf;
        this.fsns = fsns;
        atomicLRU = new AtomicReference<LRUList>();
        String defaultReplication = conf.get("dfs.replication");
        if(defaultReplication != null)
            this.DEFAULT_REPLICATION = Integer.parseInt(defaultReplication);
        String gcInterval = conf.get("dfs.replcation.gc.interval");
        if(gcInterval != null)
            this.GC_INTERVAL = Integer.parseInt(gcInterval);
        cleanUpThread = new CleanUpThread();
    }
    void touchFile(String fileName, HdfsFileStatus stat){
        this.atomicLRU.get().touchFile(fileName, stat);
    }
    void touchFile(String fileName, FileStatus stat){
        this.atomicLRU.get().touchFile(fileName, stat);
    }
    public class CleanUpThread implements Runnable{
        @Override
        public void run() {
            long gcTime = System.currentTimeMillis();
            LRUList lruList = atomicLRU.get();
            do {
                LRUList.Data data = lruList.pqueue.peek();
                if (data != null && data.time - gcTime > GC_INTERVAL) {
                    data.replication -= 1;
                    try {
                        fsns.setReplication(data.fileName, data.replication);
                        if (data.replication == DEFAULT_REPLICATION) {
                            lruList.pqueue.remove(data);
                            lruList.lookupTable.remove(data);
                        } else {
                            lruList.pqueue.remove(data);
                            data.time += GC_INTERVAL;
                            lruList.pqueue.add(data);
                        }
                        auditLog.info("Reducing replication of file " + data.fileName + " to " + data.replication);
                    } catch (Exception e) {
                        auditLog.info("GC replication cleanup Failed: " + e.getMessage());
                    }
                }
            } while (true);
        }
    }
}
