import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogAnalysis {

    public static class LogMapper
	        extends Mapper<Object, Text, Text, IntWritable> {

	    private final static IntWritable one = new IntWritable(1);
	    private Text status = new Text();

	    public void map(Object key, Text value, Context context)
	            throws IOException, InterruptedException {

	        String line = value.toString();

	        // Split by space
	        String[] parts = line.split(" ");

	        // Loop from end to find a 3-digit status code
	        for (int i = parts.length - 1; i >= 0; i--) {
	            if (parts[i].matches("\\d{3}")) {
	                status.set(parts[i]);
	                context.write(status, one);
	                break;
	            }
	        }
	    }
	}

    public static class LogReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: LogAnalysis <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log Analysis");

        job.setJarByClass(LogAnalysis.class);

        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
