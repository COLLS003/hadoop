import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemLocationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<Integer, List<String>> topLocations = new HashMap<>();
    private final int TOP_N = 10;

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
  
            throws IOException, InterruptedException {
    System.out.println("hello ...");

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        System.out.println("Key: " + key.toString() + ", Sum: " + sum);

        // Add location to the list for its count
        List<String> locations = topLocations.getOrDefault(sum, new ArrayList<>());
        locations.add(key.toString());
        topLocations.put(sum, locations);
    }

protected void cleanup(Context context) throws IOException, InterruptedException {
        // Use a PriorityQueue to get the top N locations based on count
        PriorityQueue<Integer> pq = new PriorityQueue<>(TOP_N);
        for (int count : topLocations.keySet()) {
            pq.offer(count);
            if (pq.size() > TOP_N) {
                pq.poll(); // Remove the smallest count
            }
        }

        // Output the top N locations
        while (!pq.isEmpty()) {
            int count = pq.poll();
            for (String location : topLocations.get(count)) {
                System.out.println("Location: " + location + ", Count: " + count);
                context.write(new Text(location), new IntWritable(count));
            }
        }
    }



}
