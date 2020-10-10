package bd.homework1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {


        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "longest word[s] searcher");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(args[1]+"/part-r-00000"), conf);
        WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
        Writable value = (Writable) reader.getValueClass().newInstance();
        log.info("=====================RESULT========================");
        while (reader.next(key, value))
            System.out.println(key + ":" + value);
        reader.close();
        // print counters
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED_WORDS);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
    }
}
