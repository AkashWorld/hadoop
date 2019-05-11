package org.apache.hadoop.hdfs.server.namenode.ReplicationAdapter;

/*Khalid Akash 2019 Replication Probe*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ReplicationProbe {
    public enum ACCESS_TYPE {
        NORMAL,
        APPEND
    }
    final private Configuration conf;
    final private ReplicationManager replicationManager;
    final private ReplicationGarbageCollector garbageCollector;
    private long PROBE_INTERVAL = 5000;
    public ReplicationProbe(FSNamesystem fsns, Configuration conf) {
        this.conf = conf;
        replicationManager = new ReplicationManager(conf, fsns);
        garbageCollector = new ReplicationGarbageCollector(conf,fsns);
        final Runnable probeThread = new Runnable() {
            public void run() {
                processAccessedFiles();
            }
        };
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> probeHandle = scheduler.scheduleAtFixedRate(
                probeThread,0, PROBE_INTERVAL, TimeUnit.MILLISECONDS);
        final ScheduledFuture<?> gcHandle = scheduler.scheduleAtFixedRate(
                garbageCollector.cleanUpThread, 0, garbageCollector.GC_INTERVAL, TimeUnit.MILLISECONDS);
        String probeInterval = conf.get("dfs.replication.probe.interval");
        if(probeInterval != null){
            this.PROBE_INTERVAL = Integer.parseInt(probeInterval);
        }
    }
    private void processAccessedFiles(){
        this.replicationManager.processImmediateFileReplication();
        this.replicationManager.clearImmediateFrequencies();
    }
    public void touchFileAccess(String filename, ACCESS_TYPE type, HdfsFileStatus stat){
        int frequency = 1;
        if(type == ACCESS_TYPE.APPEND)
            frequency = 15;
        this.replicationManager.addImmediateSrc(filename, frequency, stat);
        this.garbageCollector.touchFile(filename, stat);
    }
    public void touchFileAccess(String filename, ACCESS_TYPE type, FileStatus stat){
        int frequency = 1;
        if(type == ACCESS_TYPE.APPEND)
            frequency = 15;
        this.replicationManager.addImmediateSrc(filename, frequency, stat);
        this.garbageCollector.touchFile(filename, stat);
    }
}
