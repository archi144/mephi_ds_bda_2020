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
            int tmp_length = 0;
            String tmp_word = "";
            for (int i=0; i <line.length(); i++) {
                char ch = line.charAt(i);
                IS_ASCII = String.valueOf(ch).matches("\\A\\p{ASCII}*\\z");
                if (!IS_ASCII) {
                    if (tmp_length == 0) {
                        continue;
                    }
                    else {
                        word.set(tmp_word);
                        length.set(tmp_length);
                        context.write(word, length);
                        tmp_length = 0;
                        tmp_word = "";
                    }
                }
                else {
                    tmp_word += String.valueOf(ch);
                    tmp_length += 1;
                }
            }
            word.set(tmp_word);
            length.set(tmp_length);
            context.write(word, length);
        }
    }
}
