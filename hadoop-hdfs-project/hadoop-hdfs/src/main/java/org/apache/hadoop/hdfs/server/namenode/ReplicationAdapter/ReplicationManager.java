package org.apache.hadoop.hdfs.server.namenode.ReplicationAdapter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

public class ReplicationManager {
    public static final Log auditLog = LogFactory.getLog(
            ReplicationManager.class.getName() + ".audit");
    private final Configuration conf;
    private final FSNamesystem fsns;
    private int MAX_REPLICATION = 10;
    private int DENSITY_REQUIREMENT = 100;
    private Map<String, Integer> immediateFrequencies;
    private Map<String, Integer> replicationInfo;
    ReplicationManager(Configuration conf, FSNamesystem fsns) {
        this.conf = conf;
        this.fsns = fsns;
        this.immediateFrequencies = new HashMap<String, Integer>();
        String densityRequirement = conf.get("dfs.density.requirement");
        if(densityRequirement != null)
            this.DENSITY_REQUIREMENT = Integer.parseInt(densityRequirement);
        String maxReplication = conf.get("dfs.replication.maxamount");
        if(maxReplication != null)
            this.MAX_REPLICATION = Integer.parseInt(maxReplication);
    }
    void clearImmediateFrequencies(){
        this.immediateFrequencies.clear();
        this.replicationInfo.clear();
    }
    void addImmediateSrc(String fileName, int frequency, HdfsFileStatus stat){
        if(this.immediateFrequencies.containsKey(fileName) &&
                !this.replicationInfo.containsKey(fileName)){
            Integer oldFreq = this.immediateFrequencies.get(fileName);
            this.immediateFrequencies.put(fileName, oldFreq + frequency);
        }
        else{
            this.immediateFrequencies.put(fileName, frequency);
            this.replicationInfo.put(fileName, (int) stat.getReplication());
        }
    }
    void addImmediateSrc(String fileName, int frequency, FileStatus stat){
        if(this.immediateFrequencies.containsKey(fileName) &&
                !this.replicationInfo.containsKey(fileName)){
            Integer oldFreq = this.immediateFrequencies.get(fileName);
            this.immediateFrequencies.put(fileName, oldFreq + frequency);
        }
        else{
            this.immediateFrequencies.put(fileName, frequency);
            this.replicationInfo.put(fileName, (int) stat.getReplication());
        }
    }
    void processImmediateFileReplication() {
        if (immediateFrequencies == null)
            return;
        for (Map.Entry<String, Integer> entry : immediateFrequencies.entrySet()) {
            String fileName = entry.getKey();
            Integer frequency = entry.getValue();
            Integer currentReplication = replicationInfo.get(fileName);
            int requiredDensity = currentReplication * DENSITY_REQUIREMENT;
            if (currentReplication < MAX_REPLICATION && (frequency / requiredDensity) > 0) {
                try {
                    fsns.setReplication(fileName, (short) (currentReplication + 1));
                    auditLog.info("Increasing replication of file " + fileName + " to " + currentReplication + 1);
                } catch (IOException e) {
                    auditLog.info("Failure setting replication for file " + fileName + ", " + e.getMessage());
                }
            }
        }
    }
}
