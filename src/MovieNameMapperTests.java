import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class MovieNameMapperTests {
    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;

    @Before
    public void setUp() {
        MovieNameMapper mapper = new MovieNameMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("3,Grumpier Old Men (1995),Comedy|Romance"));
        mapDriver.withOutput(new IntWritable((3)), new Text("title\tGrumpier Old Men (1995)"))
        mapDriver.runTest();
    }
}
