import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

class LRUList{
    class Data implements Comparable<Data>{
        String fileName;
        long time;
        int replication;
        public int compareTo(Data d){
            return (int) (this.time - d.time);
        }
    }
    PriorityQueue<Data> pqueue;
    Map<String, Data> lookupTable;
    LRUList(){
      pqueue = new PriorityQueue<Data>();
      lookupTable = new HashMap<String, Data>();
    }
}
class ReplicationGarbageCollector {
    Map<String, Integer> replicationMap;
    ReplicationGarbageCollector(){
        replicationMap = new HashMap<String, Integer>();
    }
}
