import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class WordCount {

	/*
	public static class TextInputFormat extends FileInputFormat<LongWritable, Text> {
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
			String delimiter = "\n\n\n\n\n";
			byte[] recordDelimiterBytes = null;
			if (null != delimiter) {
				recordDelimiterBytes = delimiter.getBytes();
			}
			return new LineRecordReader(recordDelimiterBytes);
		}

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
			return codec == null;
		}
	}
	*/

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split("\n");

			for (String eachUser : line) {
				if (eachUser.matches("https://www.reddit.com/.*/comments/[a-zA-z0-9]{6}/.*/")) {
					word.set(eachUser);
					continue;
				}

				context.write(word, new Text(eachUser + " "));
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		private String str;

  	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			str = "";

			for (Text val : values) {
				str += val.toString() + " ";
			}
		    context.write(key, new Text(str));
	    }
	}

	// for step 2
	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, LongWritable> {

	    private final static LongWritable one = new LongWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String[] totalPost = value.toString().split("\n\n");
			String link = "";

			out:
	 		for (String comments : totalPost) {
				String[] line = comments.split("\n");

				for (String eachUser : line) {
					if (eachUser.matches("https://www.reddit.com/.*/comments/[a-zA-z0-9]{6}/.*/")) {
						link = eachUser;
						continue out;
					}

					// date
					if (eachUser.matches(("[a-zA-Z]{3} [a-zA-Z]{3} \\d{1,2} \\d{2}:\\d{2}:\\d{2} 201\\d UTC"))) {
						continue;
					}

					String[] eachComments = eachUser.split(" |\t|\\.|,|\\?|”|“|„|!|‘|\\(|\\)|;|~");

					for (String term : eachComments) {
						word.set(term);
						context.write(word, one);
					}
				}
      		}
		}
	}

	// for step 2
	public static class IntSumReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {
	    private LongWritable result = new LongWritable();

  	 	public void reduce(Text key, Iterable<LongWritable> values, Context context
            ) throws IOException, InterruptedException {
			int sum = 0;
    		for (LongWritable val : values) {
        		sum += val.get();
      		}
      		result.set(sum);
		    context.write(key, result);
	    }
	}

	public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "\n\n\n\n\n");
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\n");
    	Job job = Job.getInstance(conf, "word count");
    	job.setJarByClass(WordCount.class);
    	job.setMapperClass(TokenizerMapper.class);
    	job.setCombinerClass(IntSumReducer.class);
    	job.setReducerClass(IntSumReducer.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
