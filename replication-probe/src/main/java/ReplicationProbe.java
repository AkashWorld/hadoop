/*Khalid Akash 2019 Replication Probe*/
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ReplicationProbe {
    final private ReplicationManager replicationManager;
    final private String HADOOP_HOME;
    final private long PROBE_INTERVAL = 5000;
    public ReplicationProbe() {
        final long INTERVAL = PROBE_INTERVAL/1000;
        HADOOP_HOME = System.getenv("HADOOP_HOME");
        if(HADOOP_HOME == null) {
            System.err.println("Please set HADOOP_HOME as an environmental variable!");
        }
        System.out.println("HADOOP_HOME="+HADOOP_HOME);
        replicationManager = new ReplicationManager();
        final Runnable probeThread = new Runnable() {
            public void run() {
                probeLog();
            }
        };
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final ScheduledFuture<?> probeHandle = scheduler.scheduleAtFixedRate(
                probeThread,0, INTERVAL, TimeUnit.SECONDS);
    }
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
    private String getFSUserActivitySrc(String log){
        if(!log.matches(".*org.apache.hadoop.hdfs.server.namenode.FSNamesystem"+
                ".audit: allowed=true\tugi=.* \\(auth:SIMPLE\\)\tip=.*\tcmd=((?!setReplication))"+
                ".*\tsrc=.*proto=rpc")){
            return null;
        }
        int start = log.indexOf("src=") + 4;
        int end = log.indexOf('\t', start);
        if(start >= log.length() || end >= log.length() || start == end) {
            System.err.println("getFSUserActivity: substring indexing error with " + log);
            return null;
        }
        return log.substring(start, end);
    }
    private boolean isAppendCommand(String log){
       if(log == null)
           return false;
       return log.contains("cmd=append");
    }
    private void processLog(File logFile) {
        if(logFile == null) {
            System.err.println("Log given is null!");
        }
        long startTime = System.currentTimeMillis();
        System.out.println("Processing log: " + logFile.getName());
        BufferedReader reader;
        try {
            reader = new BufferedReader(new InputStreamReader(new ReverseLineInputStream(logFile)));
            while(true) {
                String line = reader.readLine();
                if(line == null)
                    break;
                if(!validateLogByTime(startTime, line))
                    break;
                String validFileSource = getFSUserActivitySrc(line);
                if(validFileSource == null)
                    continue;
                if(isAppendCommand(line))
                    this.replicationManager.addImmediateSrc(validFileSource, 15);
                else
                    this.replicationManager.addImmediateSrc(validFileSource);
            }
            reader.close();
            System.out.println("Processing finished for this round...sleeping...");
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
    private void probeLog() {
        this.replicationManager.clearImmediateFrequencies();
        System.out.println("Beginning probe...");
        File logDir = new File(HADOOP_HOME + "/logs");
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
        this.replicationManager.processImmediateFileReplication();
    }
    public static void main(String args[]) {
        ReplicationProbe repProbe = new ReplicationProbe();
    }
}
