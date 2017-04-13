import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class MovieNamesRatings {

    public static void main(String[] args) throws Exception {
        // Check that the input and output path have been provided
        if (args.length != 3) {
            System.err.println("Syntax: MovieNamesRatings <ratings input path> <titles input path> <output path>");
            System.exit(-1);
        }

        // Create an instance of the MapReduce job
        Job job = new Job();
        job.setJarByClass(MovieNamesRatings.class);
        job.setJobName("Average movie rating by movie title");

        // Set input and output locations
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieNameRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieNameMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Set reducer classes
        job.setReducerClass(MovieNameRatingReducer.class);

        // Set output key/value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Run the job and then exit the program
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
