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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


class SECFileRecordReader extends RecordReader<IntWritable, Text> {

    private static String accessionTag = new String( "ACCESSION NUMBER:" );
    
    private static String headerSTag = new String( "<SEC-HEADER>" );
    private static String headerETag = new String( "</SEC-HEADER>" );
    private static String documentSTag = new String( "<DOCUMENT>" );
    private static String documentETag = new String( "</DOCUMENT>" );
    private static String filenameSTag = new String( "<FILENAME>" );
    

    private String            accessionNumber = null;
    private Configuration     conf;
    private String            contents;
    private FileSystem        fs  = null;
    private Path              filePath;
    private FileSplit         fileSplit;
    private FSDataInputStream in;
    private boolean           read;
    private Text              value;
    private int               startLoc;
    private int               state;
    private int               endLoc;
    private int               fileLength;
    private IntWritable       nextKey;
    private Text              nextValue;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
	throws IOException, InterruptedException {
	this.accessionNumber = null;
	this.conf = context.getConfiguration();
	this.endLoc = 0;
	this.fileLength = 0;
	this.fileSplit = (FileSplit) split;
	this.fs = null;
	this.read = false;
	this.startLoc = 0;
	this.state = 0;
	this.value = null;
	this.nextKey = null;
	this.nextValue = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
	if (!read) {
	    byte[] fileData = new byte[(int) fileSplit.getLength()];
	           filePath = fileSplit.getPath();
		   fs = filePath.getFileSystem(conf);
		   in = null;
	    try {
		in = fs.open(filePath);
		IOUtils.readFully(in, fileData, 0, fileData.length);
		contents = new String( fileData, "UTF-8");
		fileLength = contents.length();
		// System.out.println( "nextKeyValue_1:  read "+Long.toString(contents.length())+" bytes" );
	    } finally {
		IOUtils.closeStream(in);
	    }
	    getCurrentKeyValue();
	    read = true;
	    return ( nextKey != null ) ? true : false;
	} else {
	    getCurrentKeyValue();
	    // System.out.println( "nextKeyValue_2:  endLoc="+endLoc+"  fileLength="+fileLength);
	    return ( nextKey != null ) ? true : false;
	}
    }

    /**
     * getCurrentKeyValue - parse through the content, looking for the next key location
     */
    public void getCurrentKeyValue() throws IOException, InterruptedException {
	int offset = 0;
	// System.out.println("getCurrentKeyValue: state = "+state+"  startLoc= "+startLoc+"  endLoc= "+endLoc);
	if ( endLoc < fileLength ) {
	    if ( state == 0 ) {
		startLoc = 0;
		endLoc   = 0;
	        offset = contents.indexOf(headerSTag,startLoc);
		if ( offset != -1 ) {
		    startLoc = offset;
		    offset = contents.indexOf(headerETag,offset);
		    if ( offset != -1 ) {
			endLoc = offset + headerETag.length();
		    }
		}
		if ( endLoc != 0 ) {
		    state = 1;
		    nextKey   = new IntWritable( SECObjectType.HEADER.to_int( ));
		    nextValue = new Text( contents.substring( startLoc, endLoc ));
		    startLoc = endLoc;
		}
	    } else if ( state == 1 ) {
		startLoc = endLoc+1;
		offset = contents.indexOf(documentSTag,startLoc);
		if ( offset != -1 ) {
		    startLoc = offset;
		    offset = contents.indexOf(documentETag,offset);
		    if ( offset != -1 ) {
			endLoc = offset;
			nextKey   = new IntWritable( SECObjectType.DOCUMENT.to_int());
			nextValue = new Text( contents.substring( startLoc, endLoc ));
			startLoc = endLoc;
		    }
		} else {
		    startLoc = fileLength+1;
		    endLoc = fileLength+1;
		    state = 2;
		    nextKey = null;
		    nextValue = null;
		}
	    }
	}
    }

    @Override
    public IntWritable getCurrentKey() throws IOException,
					 InterruptedException {
	return nextKey;
    }

    @Override
    public Text getCurrentValue() throws IOException,
					 InterruptedException {
	return nextValue;
    }

    @Override
    public float getProgress() throws IOException {
	return (float) startLoc / (float) fileLength;
    }

    @Override
    public void close() throws IOException {
	// do nothing
    }

    private String getAccessionNumber(int sLoc) {
	int i = sLoc;
	int start = 0;
	int end   = 0;

	if ( sLoc < fileLength) {
	    char c = contents.charAt(i);
	    while ( ! Character.isDigit(c) && (i+1) < fileLength ) {
		i++;
		c = contents.charAt(i);
	    }
	    start = i;
	    while (( Character.isDigit(c) || c == '-')  && (i+1) < fileLength ) {
		i++;
		c = contents.charAt(i);
	    }
	    end = i-1;
	    if ( i < fileLength && start < end ) {
		return contents.substring( start, end );
	    } else {
		return null;
	    }
	} else {
	    return null;
	}
    }

    private String getFileName(int sLoc) {
	int i = sLoc;
	int start = 0;
	int end   = 0;

	// System.out.println("SECFileRecordReader.getFileName.a: sLoc="+sLoc+"  fileLength="+fileLength );
	if ( sLoc < fileLength) {
	    char c = contents.charAt(i);
	    start = i;
	    while ( !Character.isWhitespace(c) && (i+1) < fileLength ) {
		i++;
		c = contents.charAt(i);
	    }
	    end = i;
	    // System.out.println("SECFileRecordReader.getFileName.b: start="+start+"  end="+end );
	    if ( i < fileLength && start < end ) {
		return contents.substring( start, end );
	    } else {
		return null;
	    }
	} else {
	    return null;
	}
    }
}

