import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MovieNameMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row of csv
        if (key.get() == 0 && value.toString().contains("movieId")) {
            return;
        }

        String line = value.toString();
        String[] moviePieces = line.split(",");

        if (moviePieces.length >= 2) {
            // ratingPieces is: [movieId,title,genres], want (movieId, title)
            int movieId = Integer.parseInt(moviePieces[0]);
            String title = moviePieces[1];

            // Convert to Hadoop types (mark this as the "title" value)
            IntWritable mapKey = new IntWritable(movieId);
            Text mapValue = new Text("title\t" + title);

            // Output intermediate key,value pair
            context.write(mapKey, mapValue);
        }
    }
}
