import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileStresser {
    private FileSystem fs;
    private Path rootPath;
    FileStresser(){
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            System.out.println("ERROR: $HADOOP_HOME is not found in this system!");
            return;
        }
        Path coreSitePath = new Path(hadoopHome + "/etc/hadoop/core-site.xml");
        Path hdfsSitePath = new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml");
        System.out.println(coreSitePath);
        System.out.println(hdfsSitePath);
        Configuration conf = new Configuration();
        conf.addResource(coreSitePath);
        conf.addResource(hdfsSitePath);
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        rootPath = new Path(conf.get("fs.defaultFS"));
        try {
            fs = FileSystem.get(conf);
        }
        catch(Exception e){
            System.out.println("Error: " + e.getMessage());
        }
    }
    private static void sleep(long ms){
        try{
            Thread.sleep(ms);
        }catch(Exception e){
            System.err.println(e.getMessage());
        }
    }
    private static String generateRandomString(int count){
        if(count < 1){
            return null;
        }
        int leftLimit = 65; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            if(randomLimitedInt > 90 && randomLimitedInt < 97){
                i -= 1;
                continue;
            }
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }
    void fileCreationBenchmark() throws IOException {
        long start = System.nanoTime();
        Path fileDir = new Path(rootPath.toString() + "/fileCreationDir");
        if(fs.exists(fileDir)){
            System.out.println("Exiting directory detected, deleting...");
            fs.delete(fileDir, true);
        }
        fs.mkdirs(fileDir);
        for(int i = 0; i < 64; ++i) {
            Path newFile = new Path(fileDir.toString() + "/"
                    + generateRandomString(16));
            if(fs.exists(newFile)){
                i -= 1;
                continue;
            }
            FSDataOutputStream outputStream = fs.create(newFile);
            String content = generateRandomString(4096);
            if(content == null) {
                System.out.println("Error retrieving generating string!");
                return;
            }
            outputStream.writeBytes(content);
            outputStream.close();
        }
        long end = System.nanoTime();
        long totalTime = end - start;
        System.out.println("Total elapsed time for FileCreationBenchmark: "
                + ((float)totalTime)/1000000000.0f + " s.");
        fs.delete(fileDir, true);
    }
    void operationTimingBenchmark() throws IOException {
        Path fileDir = new Path(rootPath.toString() + "/timingBenchmark");
        if(fs.exists(fileDir)){
            System.out.println("Exiting directory detected, deleting...");
            fs.delete(fileDir, true);
        }
        fs.mkdirs(fileDir);
        List<Path> filePaths = new ArrayList<Path>();
        long startCreate = System.nanoTime();
        for(int i = 0; i < 100; i++)
        {
            Path newPath = new Path(generateRandomString(16));
            if(fs.exists(newPath)){
                i--;
                continue;
            }
            filePaths.add(newPath);
            FSDataOutputStream outputStream = fs.create(newPath);
            outputStream.close();
        }
        long endCreate = (System.nanoTime() - startCreate)/100;
        System.out.println("Creation of 100 files took an average of: " + endCreate + " ns");
        long startAppend = System.nanoTime();
        for(Path path : filePaths){
            FSDataOutputStream ostream = fs.append(path);
            ostream.write('A');
            ostream.close();
        }
        long endAppend = System.nanoTime() - startAppend;
        System.out.println("Append of 100 files took an average of: " + endAppend + " ns");
        long startOpen = System.nanoTime();
        for(Path path : filePaths){
            FSDataInputStream istream = fs.open(path);
            int status = istream.read();
            istream.close();
        }
        long endOpen = System.nanoTime() - startOpen;
        System.out.println("Opening of 100 files took an average of: " + endOpen + " ns");
        fs.delete(fileDir, true);
        System.out.println("Creation took: " + endCreate/endAppend + " than append and " +
                endCreate/endOpen + " than open");
        System.out.println("Append took: " + endAppend/endCreate + " than create and " +
                endAppend/endOpen + " than open");
        System.out.println("Opens took: " + endOpen/endCreate + " than create and " +
                endOpen/endAppend + " than append");
    }

    void replicationChangerBmark() throws IOException{
        long start = System.nanoTime();
        Path fileDir = new Path(rootPath.toString() + "/replicationChanger");
        if(fs.exists(fileDir)){
            System.out.println("Exiting directory detected, deleting...");
            fs.delete(fileDir, true);
        }
        fs.mkdirs(fileDir);
        List<Path> filePaths = new ArrayList<Path>();
        for(int i = 0; i < 32; i++)
        {
            Path newPath = new Path(generateRandomString(8));
            if(fs.exists(newPath)){
                i--;
                continue;
            }
            filePaths.add(newPath);
            FSDataOutputStream outputStream = fs.create(newPath);
            String content = generateRandomString(4096);
            if(content == null) {
                System.out.println("Error retrieving generating string!");
                return;
            }
            outputStream.writeBytes(content);
            outputStream.close();
        }
        sleep(5000);
        Random random = new Random();
        System.out.println("Appending without opening and closing");
        for(Path path : filePaths)
        {
            FSDataOutputStream appendStream = fs.append(path);
            for(int i = 0; i < 1000; ++i)
            {
                appendStream.writeByte('A');
            }
            appendStream.close();
        }
        sleep(5000);
        System.out.println("Appending with opening and closing");
        for(Path path : filePaths)
        {
            for(int i = 0; i < 1000; ++i)
            {
                FSDataOutputStream appendStream = fs.append(path);
                appendStream.writeByte('A');
                appendStream.close();
            }
        }
        sleep(5000);
        System.out.println("Opening and reading");
        for(Path path : filePaths)
        {
            for(int i = 0; i < 1000; ++i)
            {
                FSDataInputStream inputStream = fs.open(path);
                int status = inputStream.read();
                inputStream.close();
            }
        }
        long end = System.nanoTime();
        long totalTime = end - start;
        System.out.println("Total elapsed time for FileReplicationBenchmark: "
                + ((float)totalTime)/1000000000.0f + " s.");
        fs.delete(fileDir, true);
    }
}
