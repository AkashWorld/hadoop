import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicationManager {
    private final int MAX_REPLICATION;
    private int DENSITY_REQUIREMENT;
    private int DEFAULT_REPLICATION;
    private final ReplicationGarbageCollector garbageCollector;
    private Map<String, Integer> immediateFrequencies;
    private ExecutorService pool;
    ReplicationManager() {
        this.MAX_REPLICATION = 10;
        pool = Executors.newSingleThreadExecutor();
        this.garbageCollector = new ReplicationGarbageCollector();
        this.immediateFrequencies = new HashMap<String, Integer>();
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            System.out.println("ERROR: $HADOOP_HOME is not found in this system!");
            return;
        }
        DENSITY_REQUIREMENT = 100;

    }
    void clearImmediateFrequencies(){
        this.immediateFrequencies.clear();
    }
    void addImmediateSrc(String fileName){
        if(this.immediateFrequencies.containsKey(fileName)){
            Integer oldFreq = this.immediateFrequencies.get(fileName);
            this.immediateFrequencies.put(fileName, oldFreq + 1);
        }
        else{
            this.immediateFrequencies.put(fileName, 1);
        }
    }
    void addImmediateSrc(String fileName, int frequency){
        fileName = this.rootPath + fileName;
        if(this.immediateFrequencies.containsKey(fileName)){
            Integer oldFreq = this.immediateFrequencies.get(fileName);
            this.immediateFrequencies.put(fileName, oldFreq + frequency);
        }
        else{
            this.immediateFrequencies.put(fileName, frequency);
        }
    }
    private class ImmediateReplicationThread implements Runnable{
        private FileSystem fs;
        private Map<String, Integer> immediateFreqs;
        private int replicationDensityRequirement;
        private final int MAX_REPLICATION;
        ImmediateReplicationThread(FileSystem fs, Map<String, Integer> freqs,
                                   int densityRequirement, int MAX_REPLICATION){
            this.fs = fs;
            this.immediateFreqs = freqs;
            this.replicationDensityRequirement = densityRequirement;
            this.MAX_REPLICATION = MAX_REPLICATION;
        }
        public void run() {
            if(fs == null || immediateFreqs == null)
                return;
            for(Map.Entry<String, Integer> entry : this.immediateFreqs.entrySet()){
                String fileName = entry.getKey();
                Integer frequency = entry.getValue();
                try{
                    Path filePath = new Path(fileName);
                    FileStatus fileStatus = fs.getFileStatus(filePath);
                    int currentReplication = fileStatus.getReplication();
                    int requiredDensity = currentReplication * replicationDensityRequirement;
                    if(currentReplication < MAX_REPLICATION && (frequency/requiredDensity) > 0){
                        System.out.println("Setting replication of " + filePath.getName() +
                                " to " + (currentReplication + 1));
                        fs.setReplication(filePath, (short) (currentReplication + 1));
                    }
                }
                catch(IOException e){
                    System.err.println("Attempted to set replication, but file was deleted already.");
                }
            }
        }
    }
    void processImmediateFileReplication(){
        ImmediateReplicationThread replicator = new
                ImmediateReplicationThread(this.fs, this.immediateFrequencies,
                 this.DENSITY_REQUIREMENT, this.MAX_REPLICATION);
        pool.execute(replicator);
    }
}
