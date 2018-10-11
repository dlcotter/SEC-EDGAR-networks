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
    extends FileInputFormat<Text, Text> {
  
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
	return false;
    }

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
      @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      List<InputSplit> result = new ArrayList<InputSplit>();
      Path outDir = FileOutputFormat.getOutputPath(job);
      int numSplits = 
            job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      for(int i=0; i < numSplits; ++i) {
        result.add(new FileSplit(new Path(outDir, "sec-doc-" + i), 0, 1, 
                                  (String[])null));
      }
      return result;
    }
     */
 
  @Override
  public RecordReader<Text, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
      SECFileRecordReader reader = new SECFileRecordReader();
      reader.initialize(split, context);
      return reader;
  }
}

