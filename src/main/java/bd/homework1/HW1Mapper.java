package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Маппер: парсит UserAgent из логов, к каждому браузеру присваиваем в соответствие единицу
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable length = new IntWritable();
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean IS_ASCII = line.matches("\\A\\p{ASCII}*\\z");
        if (IS_ASCII) {
            length.set(line.length());
            word.set(line);
            context.write(word, length);
        } else {
            context.getCounter(CounterType.MALFORMED).increment(1);
            int tmp_length = 1;
            int lower_threshold = 0;
            for (int i=0; i <line.length(); i++) {
                char ch = line.charAt(i);
                IS_ASCII = String.valueOf(ch).matches("\\A\\p{ASCII}*\\z");
                if (!IS_ASCII) {
                    tmp_length = i - lower_threshold;
                    word.set(line.substring(lower_threshold,i));
                    length.set(tmp_length);
                    lower_threshold = i;
                    context.write(word, length);
                }
                else {
                    tmp_length += 1;
                }
            }
        }
    }
}
