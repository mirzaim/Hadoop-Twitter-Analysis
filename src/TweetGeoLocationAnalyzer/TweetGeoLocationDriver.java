package TweetGeoLocationAnalyzer;

import java.io.IOException;

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

public class TweetGeoLocationDriver {

    public static class TweetCountryLocation2Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String candidate = ((FileSplit) context.getInputSplit()).getPath().getName().split("\\.")[0];
            String[] row = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            try {
                float latitude = Float.parseFloat(row[13]);
                float longitude = Float.parseFloat(row[14]);

                if (((-161.75 < longitude) && (longitude < -68)) && ((19.5 < latitude) && (latitude < 64.85))) {
                    context.write(new Text("america"), new Text(candidate));
                } else if (((-4.65 < longitude) && (longitude < 9.45)) && ((41.6 < latitude) && (latitude < 51))) {
                    context.write(new Text("france"), new Text(candidate));
                }
            } catch (NumberFormatException e) {
            }

        }
    }

    public static class TweetCountryLocation2Reducer extends Reducer<Text, Text, Text, Text> {

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
        job.setJarByClass(TweetCountryLocation2Driver.class);
        job.setMapperClass(TweetCountryLocation2Mapper.class);
        // job.setCombinerClass(TweetCountryLocationReducer.class);
        job.setReducerClass(TweetCountryLocation2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}