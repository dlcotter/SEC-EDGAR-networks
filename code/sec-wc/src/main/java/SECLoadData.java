import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.util.*;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.helpers.DefaultHandler;

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

public class SECLoadData extends Configured implements Tool  {

    private static final Logger           LOG       = Logger.getLogger(SECLoadData.class);
    
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new SECLoadData(), args);
	System.exit(res);
    }

    public int run(String[] args) throws Exception {
	Job job = Job.getInstance(getConf(), "secloaddata");
	job.setJarByClass(this.getClass());
	//job.setInputFormatClass(SECFileInputFormat.class);
	job.setInputFormatClass(WholeFileInputFormat.class);
	// Use TextInputFormat, the default unless job.setInputFormatClass is used
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<NullWritable, Text, Text, Text> {
	private Text word = new Text();
	public void map(NullWritable key, Text fileData, Context context)
	    throws IOException, InterruptedException {
	    String content     = fileData.toString();
	    SECObjectFiling filing = new SECObjectFiling( content );
	    // System.out.println( "Map.map: recordType = "+ recordType+"  content length = "+Long.toString(content.length()));

	    ArrayList<SECObject> secObjects = filing.parse( content, 0, content.length());
	    if ( secObjects != null && secObjects.size() > 0 ) {
		for ( SECObject item : secObjects ) {
		    SECObjectType itemType  = item.getType();
		    if ( itemType != SECObjectType.NONE ) {
			String        tableName = itemType.to_string();
			String        csv_line  = item.toCSV();
			if ( tableName != null && 
			     tableName.length() > 1 &&
			     csv_line.length() > 5 ) {
			    System.out.println( "Map: csv="+csv_line );
			    context.write(new Text(tableName),new Text(csv_line));
			}
		    }
		}
	    }
	}
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text word, Iterable<Text> csv_lines, Context context)
	    throws IOException, InterruptedException {
	    int sum = 0;
	    for (Text line : csv_lines) {
		context.write(word, line);
	    }
	}
    }
}
