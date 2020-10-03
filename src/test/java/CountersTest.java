import bd.homework1.CounterType;
import bd.homework1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author Mechetin Arthur
 * @since 1/10/2020
 */
public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String MalforedString1 = "Я вам не ASCII!";
    private final String MalforedString2 = "Я вредина!";
    private final String GoodString = "I am ASCII!";
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterThree() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(MalforedString1))
                .withOutput(new Text(), new IntWritable(0))
                .withOutput(new Text(), new IntWritable(0))
                .withOutput(new Text(), new IntWritable(0))
                .withOutput(new Text("ASCII"), new IntWritable(5))
                .runTest();
        assertEquals("Expected 3 counter increment", 3, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED_WORDS).getValue());
    }

    @Test
    public void testMapperCounterTwo() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(MalforedString2))
                .withOutput(new Text(), new IntWritable(0))
                .withOutput(new Text(), new IntWritable(0))
                .runTest();
        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED_WORDS).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(GoodString))
                .withOutput(new Text("I"), new IntWritable(1))
                .withOutput(new Text("am"), new IntWritable(2))
                .withOutput(new Text("ASCII"), new IntWritable(5))
                .runTest();
        assertEquals("Expected 0 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED_WORDS).getValue());
    }
}

