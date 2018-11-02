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
    // private static final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    // private static SAXParser saxParser;
    // saxParser = saxParserFactory.newInstance();

    
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new SECLoadData(), args);
	System.exit(res);
    }

    public int run(String[] args) throws Exception {
	Job job = Job.getInstance(getConf(), "secloaddata");
	job.setJarByClass(this.getClass());
	job.setInputFormatClass(SECFileInputFormat.class);
	// Use TextInputFormat, the default unless job.setInputFormatClass is used
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<IntWritable, Text, Text, Text> {
	private Text word = new Text();
	public void map(IntWritable key, Text fileData, Context context)
	    throws IOException, InterruptedException {
	    String content     = fileData.toString();
	    int    recordType  = key.get();

	    // System.out.println( "Map.map: recordType = "+ recordType+"  content length = "+Long.toString(content.length()));

	    SECObject[] secObjects = SECObjectFiling.parse( content, 0, content.length());
	    if ( secObjects != null ) {
		for ( int i = 0; i < secObjects.length; i++ ) {
		    SECObject item      = secObjects[i];
		    int       itemType  = SECObjectType.to_int( item.getType());
		    String    tableName = null;
		    String    csv_line  = item.toCSV();

		    switch (itemType) {
		    case 0:    tableName="missing";    break;
		    case 1:    tableName="contacts";   break;
		    case 2:    tableName="documents";  break;
		    case 3:    tableName="entities";   break;
		    case 4:    tableName="filings";    break;
		    case 5:    tableName="form10ks";   break;
		    case 6:    tableName="form4s";     break;
		    case 7:    tableName="form8ks";    break;
		    case 8:    tableName="headers";    break;
		    case 9:    tableName="owner_rels"; break;
		    default:   tableName="missing";    break;
		    }
		
		    context.write(new Text(tableName),new Text(csv_line));
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
