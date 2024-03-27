import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ItemLocationDriver {

    public static void main(String[] args) throws Exception {
        System.out.println(args.length);
        if (args.length != 3) {

            System.err.println("Usage: ItemLocationDriver <input path> <output path> <item name>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("item.name", args[2]);

        Job job = Job.getInstance(conf, "Item Location");
        job.setJarByClass(ItemLocationDriver.class);
        job.setMapperClass(ItemLocationMapper.class);
        job.setReducerClass(ItemLocationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
