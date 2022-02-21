package graal.systems.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class ExampleRunner {
    public static void main(String... args) throws IOException {

        Configuration configuration = new Configuration(false);
        configuration.addResource(ExampleRunner.class.getClassLoader().getResourceAsStream("core-site.xml"));
        configuration.addResource(ExampleRunner.class.getClassLoader().getResourceAsStream("yarn-site.xml"));
        configuration.addResource(ExampleRunner.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
        configuration.writeXml(System.out);

        JobConf conf = new JobConf(configuration, ExampleRunner.class);
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
        JobClient.runJob(conf);
    }
}