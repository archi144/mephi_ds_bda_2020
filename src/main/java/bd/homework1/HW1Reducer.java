package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final ArrayList<String> long_words = new ArrayList<String>();
    private int max_length = Integer.MIN_VALUE;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        while (values.iterator().hasNext()) {
            int tmp_length = values.iterator().next().get();
            if (tmp_length == max_length) {
                long_words.add(word);
            }
            else if (tmp_length > max_length) {
                max_length = tmp_length;
                long_words.clear();
                long_words.add(word);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String word : long_words) {
            context.write(new Text(word), new IntWritable(max_length));
        }
    }
}
