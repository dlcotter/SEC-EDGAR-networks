import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class SECWordCount extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(SECWordCount.class);

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new SECWordCount(), args);
	System.exit(res);
    }

    public int run(String[] args) throws Exception {
	Job job = Job.getInstance(getConf(), "secwordcount");
	job.setJarByClass(this.getClass());
	job.setInputFormatClass(SECFileInputFormat.class);
	// Use TextInputFormat, the default unless job.setInputFormatClass is used
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<Text, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public void map(Text key, Text fileData, Context context)
	    throws IOException, InterruptedException {
	    String content = fileData.toString();
	    String keyval  = key.toString();
	    System.out.println( "Map.map: key = "+keyval+"  content length = "+Long.toString(content.length()));
	    StringBufferInputStream inStream = new StringBufferInputStream( content );
	    ParseSECData parser = new ParseSECData( inStream );
	    Text currentWord = new Text();
	    for (String word = parser.nextWord(); word != null; word = parser.nextWord()) {
		currentWord = new Text(word);
		//System.out.println( "Word:  "+word );
		context.write(currentWord,one);
	    }
	}
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text word, Iterable<IntWritable> counts, Context context)
	    throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable count : counts) {
	        sum += count.get();
	    }
	    context.write(word, new IntWritable(sum));
	}
    }
}
