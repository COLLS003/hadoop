import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemLocationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text location = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert JSON line to string
        String jsonString = value.toString();
        
        // Check if the JSON contains the requested item
        String item = context.getConfiguration().get("item.name");

        // Check if the line is not null
        if (jsonString != null && jsonString.contains(item)) {
            // Extract location from JSON
            String[] sections = jsonString.split("\"");
            for (int i = 0; i < sections.length - 1; i++) {
                if (sections[i].trim().equals(item)) {
                    String[] locationParts = sections[i - 1].split(":");
                    String locationName = locationParts[0].trim();
                    // Filter out unwanted locations
                    if (isValidLocation(locationName)) {
                        location.set(locationName);
                        context.write(location, one);
                        break;
                    }
                }
            }
        }
    }

    // Function to validate if a location is part of the desired sections
    private boolean isValidLocation(String locationName) {
        String[] validLocations = {"Light World", "Eastern Palace", "Desert Palace", "Death Mountain", 
            "Tower Of Hera", "Castle Tower", "Dark World", "Dark Palace", "Swamp Palace", "Skull Woods", 
            "Thieves Town", "Ice Palace", "Misery Mire", "Turtle Rock", "Ganons Tower"};
        for (String validLocation : validLocations) {
            if (locationName.contains(validLocation)) {
                return true;
            }
        }
        return false;
    }
}
