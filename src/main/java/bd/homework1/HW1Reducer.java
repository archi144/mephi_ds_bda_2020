package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Reducer: Gets words by {@link HW1Mapper} and keep longest word[s]
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final ArrayList<String> long_words = new ArrayList<String>();
    private int max_length = Integer.MIN_VALUE;

    /**
     * Reduce methond searches longest word[s] and write its to ArrayList<String> also kept the biggest length of words
     * @param key key from {@link HW1Mapper} output
     * @param values values from {@link HW1Mapper} output
     * @param context MapReduce job context
     * @throws IOException          Throws on context.write
     * @throws InterruptedException Throws on context.write
     */
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

    /**
     * Cleanup method for MapReduce process, which write all longest words into context
     *
     * @param context MapReduce job context
     * @throws IOException          Throws on context.write
     * @throws InterruptedException Throws on context.write
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String word : long_words) {
            context.write(new Text(word), new IntWritable(max_length));
        }
    }
}
