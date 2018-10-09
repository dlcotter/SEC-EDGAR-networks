/**
 *  SECFileRecordReader The RecordReader used by SECFileInputFormat 
 *  for reading a document within the file as a record
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


class SECFileRecordReader extends RecordReader<Text, Text> {
  
    private FileSplit     fileSplit;
    private Path          filePath;
    private Configuration conf;
    private Text          value = new Text();
    private boolean       processed = false;
    private String        contents;

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
	    filePath = fileSplit.getPath();
	    FileSystem fs = file.getFileSystem(conf);
	    FSDataInputStream in = null;
	    try {
		in = fs.open(file);
		IOUtils.readFully(in, fileData, 0, fileData.length);
		contents = new String( fileData, "UTF-8");
		System.out.println( "nextKeyValue_1:  read "+Long.toString(contents.length())+" bytes" );
	    } finally {
		IOUtils.closeStream(in);
	    }
	    processed = true;
	    return true;
	}
	return false;
    }

    /**
     * getCurrentKey - parse through the content, looking for 
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
	
	return key;
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
    */
}

