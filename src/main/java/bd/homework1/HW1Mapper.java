package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: get the line and write words in to job context without spec symbols and non-ASCII symbols
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable length = new IntWritable();
    private final String tokens = " #$%,.[\\]!@*+-_/:\"&();<=>^~\n\r\t";
    private Text word = new Text();



    /**
     * Map method for MapReduce process. Check
     * words on non-MALFORMED words and write good words
     * in to job context
     *
     * @param key     Key of MapReduce input, unused
     * @param value   Input text
     * @param context MapReduce job context, unused
     * @throws IOException          Throws on context.write
     * @throws InterruptedException Throws on context.write
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, tokens);
        while (tokenizer.hasMoreTokens()) {
                String str = tokenizer.nextToken();
            boolean IS_ASCII = str.matches("\\A\\p{ASCII}*\\z");
            if (IS_ASCII) {
                length.set(str.length());
                word.set(str);
                context.write(word, length);
            } else {
                context.getCounter(CounterType.MALFORMED_WORDS).increment(1);
                String tmp_word = "";
                for (int i = 0; i < str.length(); i++) {
                    char ch = str.charAt(i);
                    IS_ASCII = String.valueOf(ch).matches("\\A\\p{ASCII}*\\z");
                    if (!IS_ASCII) {
                        if (tmp_word.length() == 0) {
                            continue;
                        }
                        else {
                            word.set(tmp_word);
                            length.set(tmp_word.length());
                            context.write(word, length);
                            tmp_word = "";
                        }
                    } else {
                        tmp_word += String.valueOf(ch);
                    }
                }
                if (tmp_word.length() !=0) {
                    word.set(tmp_word);
                    length.set(tmp_word.length());
                    context.write(word, length);
                }
            }
        }
    }
}
