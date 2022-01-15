package TweetCounter;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TweetCounterDriver {

    public static class TweetCounterMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String candidate = ((FileSplit) context.getInputSplit()).getPath().getName().split("\\.")[0];
            String[] SingleCountryData = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            try {
                int likes = Integer.parseInt(SingleCountryData[3]);
                int retweet = Integer.parseInt(SingleCountryData[4]);
                context.write(new Text(candidate), new Text(likes + "\t" + retweet));
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class TweetCounterReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Text key = t_key;
            int likes = 0, retweets = 0;
            String[] temp;
            for (Text value : values) {
                // replace type of value with the actual type of our value
                temp = value.toString().split("\t");
                likes += Integer.parseInt(temp[0]);
                retweets += Integer.parseInt(temp[1]);
            }
            context.write(key, new Text(likes + "\t" + retweets));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tweet Counter");
        job.setJarByClass(TweetCounterDriver.class);
        job.setMapperClass(TweetCounterMapper.class);
        job.setCombinerClass(TweetCounterReducer.class);
        job.setReducerClass(TweetCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}