// cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv WholeFileRecordReader
class WholeFileRecordReader extends RecordReader<NullWritable, Text> {
  
  private FileSplit fileSplit;
  private Configuration conf;
  private Text value = new Text();
  private boolean processed = false;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.fileSplit = (FileSplit) split;
    this.conf = context.getConfiguration();
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!processed) {
      byte[] fileData = new byte[(int) fileSplit.getLength()];
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);
      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, fileData, 0, fileData.length);
	String contents = new String( fileData, "UTF-8");
        value = new Text(contents);
	System.out.println( "nextKeyValue_1:  read "+Long.toString(contents.length())+" bytes" );
      } finally {
        IOUtils.closeStream(in);
      }
      processed = true;
      return true;
    }
    return false;
  }
  
  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public Text getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
//^^ WholeFileRecordReader
