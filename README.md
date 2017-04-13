# movielens-mapreduce
Analyzing MovieLens movie data with MapReduce

### Computing the average rating by movie
How to run:
1. Build a jar from the source files using the main() routine in MovieRatings.java, e.g. MovieLensMapReduce.jar
2. Run the following commands:
- export HADOOP_CLASSPATH=MovieLensMapReduce.jar
- hadoop MovieRatings /data/ratings.csv /output 
3. Find the results in /outputs

### Computing the average rating by movie including movie title
How to run:
1. Build a jar from the source files using the main() routine in MovieNamesRatings.java, e.g. MovieLensNameMapReduce.jar
2. Run the following commands:
- export HADOOP_CLASSPATH=MovieLensNameMapReduce.jar
- hadoop MovieRatings /data/ratings.csv /data/movies.txt /output_titles 
3. Find the results in /output_titles
