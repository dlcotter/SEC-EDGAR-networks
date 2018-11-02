/**
 * SECInputFormat An InputFormat for reading an SEC filing
 * and splitting it into one or more component files
 */
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

public class SECFileInputFormat
    extends FileInputFormat<IntWritable, Text> {
  
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
	return false;
    }

  public RecordReader<IntWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
      SECFileRecordReader reader = new SECFileRecordReader();
      reader.initialize(split, context);
      return reader;
  }
}

