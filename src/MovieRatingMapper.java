import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] ratingPieces = line.split(",");

        if (ratingPieces.length >= 2) {
            // ratingPieces is: [userId, movieId, rating, timestamp]
            int movieId = Integer.parseInt(ratingPieces[1]);
            double rating = Double.parseDouble(ratingPieces[2]);

            // Convert to Hadoop types
            IntWritable mapKey = new IntWritable(movieId);
            DoubleWritable mapValue = new DoubleWritable(rating);

            // Output intermediate key,value pair
            context.write(mapKey, mapValue);
        }
    }
}
