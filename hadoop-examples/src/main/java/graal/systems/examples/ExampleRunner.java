package graal.systems.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount for the YARN bridge of graal (ADR 0054). The job carries no graal-specific code: the configuration
 * comes from {@code HADOOP_CONF_DIR} (the files returned by {@code GET /api/v1/bridges/{id}}) and the bridge token
 * from {@code HADOOP_TOKEN_FILE_LOCATION}. Run it with {@code hadoop jar}; {@code -D} options are honoured.
 */
public class ExampleRunner extends Configured implements Tool {

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new ExampleRunner(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: hadoop jar hadoop-examples-uber.jar graal.systems.examples.ExampleRunner <input> <output>");
            return 2;
        }
        JobConf conf = new JobConf(getConf(), ExampleRunner.class);
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(ExampleMapper.class);
        conf.setCombinerClass(ExampleReducer.class);
        conf.setReducerClass(ExampleReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        RunningJob job = JobClient.runJob(conf);
        return job.isSuccessful() ? 0 : 1;
    }
}
