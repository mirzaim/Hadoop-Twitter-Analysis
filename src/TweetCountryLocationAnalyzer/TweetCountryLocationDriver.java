package TweetCountryLocationAnalyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TweetCountryLocationDriver {

    public static class TweetCountryLocationMapper extends Mapper<Object, Text, Text, Text> {

        private static String[] countries = { "america", "iran", "netherlands", "austria", "mexico", "emirates",
                "france", "germany", "england", "canada", "spain", "italy" };

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String candidate = ((FileSplit) context.getInputSplit()).getPath().getName().split("\\.")[0];
            String[] row = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            try {
                String country = Arrays.stream(countries).filter(row[16].trim().toLowerCase()::contains).findFirst()
                        .get();
                if (country != null && !country.isEmpty()) {
                    context.write(new Text(country), new Text(candidate));
                }
            } catch (NoSuchElementException e) {
            }

        }
    }

    public static class TweetCountryLocationReducer extends Reducer<Text, Text, Text, Text> {

        private long mapperCounter;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounter = currentJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Text key = t_key;
            int biden = 0, trump = 0, all = 0;
            for (Text value : values) {
                // replace type of value with the actual type of our value
                if (value.toString().contains("biden")) {
                    biden++;
                } else if (value.toString().contains("trump")) {
                    trump++;
                }
                all++;
            }
            context.write(key, new Text((float) all / mapperCounter * 100 + "\t" + (float) biden / all * 100 + "\t"
                    + (float) trump / all * 100 + "\t" + all));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tweet Counter");
        job.setJarByClass(TweetCountryLocationDriver.class);
        job.setMapperClass(TweetCountryLocationMapper.class);
        // job.setCombinerClass(TweetCountryLocationReducer.class);
        job.setReducerClass(TweetCountryLocationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}