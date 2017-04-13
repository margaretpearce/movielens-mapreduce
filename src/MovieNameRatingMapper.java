import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieNameRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row of csv
        if (key.get() == 0 && value.toString().contains("userId")) {
            return;
        }

        String line = value.toString();
        String[] ratingPieces = line.split(",");

        if (ratingPieces.length >= 2) {
            // ratingPieces is: [userId, movieId, rating, timestamp]
            int movieId = Integer.parseInt(ratingPieces[1]);
            String rating = ratingPieces[2];

            // Convert to Hadoop types
            IntWritable mapKey = new IntWritable(movieId);
            Text mapValue = new Text("rating\t" + rating);

            // Output intermediate key,value pair
            context.write(mapKey, mapValue);
        }
    }
}