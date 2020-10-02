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

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max_length = 0;
        String word = key.toString();
        ArrayList long_words = new ArrayList();
        while (values.iterator().hasNext()) {
            if (values.iterator().next().get() > max_length)
            {
                max_length = values.iterator().next().get();
                long_words.clear();
                long_words.add(word);
            }
            else if (values.iterator().next().get() == max_length) {
                long_words.add(word);
            }
        }
        for (int i=0; i < long_words.size(); i++) {
            context.write(new Text(word), new IntWritable(max_length));
        }
    }
}
