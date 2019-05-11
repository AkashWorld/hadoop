import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ReplicationTest {
    private static class HDFSLogWatcher implements Runnable{
        int PROBE_INTERVAL = 1000;
        private boolean validateLogByTime(long startTime, String line) {
            if(line == null)
                return false;
            int endOfDateTime = line.indexOf(':') + 6;
            String dateTime = line.substring(0, endOfDateTime);
            if(!dateTime.matches(
                    "2[0-9]{3}-[0-9]{2}-[0-9]{2} ([0-9]{2}:){2}[0-9]{2}"))
                return false;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try{
                Date date = sdf.parse(dateTime);
                long dateMs = date.getTime();
                if(dateMs > startTime)
                    return false;
                else if(startTime - dateMs > PROBE_INTERVAL)
                    return false;
            }
            catch(ParseException e){
                System.err.println(e.getMessage());
                return false;
            }
            return true;
        }
        String getReplicationLog(String line){
            if(line == null){
                return null;
            }
            if(line.contains("Replication")){
                return line;
            }
            return null;
        }
        void processLog(File logFile){
            if(logFile == null) {
                System.err.println("Log given is null!");
            }
            long startTime = System.currentTimeMillis();
            System.out.println("\rProcessing log: " + logFile.getName());
            BufferedReader reader;
            try {
                reader = new BufferedReader(new InputStreamReader(new ReverseLineInputStream(logFile)));
                while(true) {
                    String line = reader.readLine();
                    if(line == null)
                        break;
                    if(!validateLogByTime(startTime, line))
                        break;
                    String replicationLog = getReplicationLog(line);
                    if(replicationLog == null)
                        continue;
                    System.out.println(replicationLog);
                }
                reader.close();
            }
            catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
        public void run() {
            String hadoopHome = System.getenv("HADOOP_HOME");
            if (hadoopHome == null) {
                System.out.println("ERROR: $HADOOP_HOME is not found in this system!");
                return;
            }
            File logDir = new File(hadoopHome + "/logs");
            if(logDir.exists() && logDir.isDirectory()) {
                File[] files = logDir.listFiles();
                if(files == null){
                    System.err.println("This directory does not contain any files!");
                    return;
                }
                for(File logFile : files) {
                    if(logFile.getName().matches("hadoop-.*-namenode-.*.log")) {
                        processLog(logFile);
                        break;
                    }
                }
            }
            else {
                System.err.println("Log directory not found!");
                return;
            }
        }
    }

    public static void main(String[] args) {
        HDFSLogWatcher watcher = new HDFSLogWatcher();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> probeHandle = scheduler.scheduleAtFixedRate(
                watcher,0, 1000, TimeUnit.MILLISECONDS);
        FileStresser fileStresser = new FileStresser();
        try{
            fileStresser.fileCreationBenchmark();
            fileStresser.operationTimingBenchmark();
            fileStresser.replicationChangerBmark();
        }
        catch(Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
