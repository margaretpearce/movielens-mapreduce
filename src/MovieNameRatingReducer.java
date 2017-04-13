import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieNameRatingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        double avgRating = 0;
        int numValues = 0;
        String title = "";

        // Add up all the ratings
        for (Text value: values) {
            // Separate the value from the tag
            String parts[] = value.toString().split("\t");

            if (parts[0].equals("title")) {
                // Get the title
                title = parts[1];
            }
            else {
                // Get a rating for this title
                double ratingValue = Double.parseDouble(parts[1]);
                avgRating += ratingValue;
                numValues++;
            }
        }

        // Divide by the number of ratings to get the average for this movie
        avgRating /= numValues;

        String outputValue = title + "\n" + avgRating;
        Text output = new Text(outputValue);

        // Output movieId, title and avg rating
        context.write(key, output);
    }
}
