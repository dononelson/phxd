package com.visa.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

/**
 * Read lines in a JSON file to produce a complete JSON record on one line.
 */
public class JSONLineRecordReader extends RecordReader {
	
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	
	private static StringBuilder jsonObject = new StringBuilder();
	private int level = 0;
	
	private static final String START_OBJ = "{";
	private static final String END_OBJ = "}";
	private static final String MARK = "<<<<<<<<<<<==============";
	
	private static final Logger log = Logger.getLogger(JSONLineRecordReader.class);
	
	public JSONLineRecordReader() {
	}
	
	/**
	 * From Design Pattern, O'Reilly... This method takes as arguments the map task’s assigned InputSplit and
	 * TaskAttemptContext, and prepares the record reader. For file-based input formats, this is a good place to seek to
	 * the byte position in the file to begin reading.
	 */
	@Override
	public void initialize(
			InputSplit genericSplit,
			TaskAttemptContext context)
			throws IOException {
		
		// This InputSplit is a FileInputSplit
		FileSplit split = (FileSplit) genericSplit;
		
		// Retrieve configuration, and Max allowed
		// bytes for a single record
		Configuration job = context.getConfiguration();
		maxLineLength = job.getInt(
				"mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		
		// Split "S" is responsible for all records
		// starting from "start" and "end" positions
		start = split.getStart();
		end = start + split.getLength();
		
		// Retrieve file containing Split "S"
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		
		// If Split "S" starts at byte 0, first line will be processed
		// If Split "S" does not start at byte 0, first line has been already
		// processed by "S-1" and therefore needs to be silently ignored
		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;
			// Set the file pointer at "start - 1" position.
			// This is to make sure we won't miss any line
			// It could happen if "start" is located on a EOL
			--start;
			fileIn.seek(start);
		}
		
		in = new LineReader(fileIn, job);
		
		// If first line needs to be skipped, read first line
		// and stores its content to a dummy Text
		if (skipFirstLine) {
			Text dummy = new Text();
			// Reset "start" to "start + line offset"
			start += in.readLine(dummy, 0,
					(int) Math.min(
							Integer.MAX_VALUE,
							end - start));
		}
		
		// Position is the actual start
		pos = start;
		
	}
	
	/**
	 * From Design Pattern, O'Reilly... Like the corresponding method of the InputFormat class, this reads a single key/
	 * value pair and returns true until the data is consumed.
	 */
	@Override
	public boolean nextKeyValue() throws IOException {
		// Current offset is the key
		key.set(pos);
		
		int newSize = 0;
		boolean keyValFound = true;
		
		log.debug(MARK + " pos = " + pos + ", end = " + end);
		// Make sure we get at least one record that starts in this Split
		while (pos < end) {
			
			// Read first line and store its content to "value"
			newSize = in.readLine(value, maxLineLength,
					Math.max((int) Math.min(
							Integer.MAX_VALUE, end - pos),
							maxLineLength));
			
			// No byte read, seems that we reached end of Split
			// Break and return false (no key / value)
			if (newSize == 0) {
				keyValFound = false;
				break;
			}
			
			keyValFound = addToJsonObj();
			
			// Line is read, new position is set
			pos += newSize;
			
			// Line is lower than Maximum record line size
			// break and return true (found key / value)
			if (newSize < maxLineLength) {
				keyValFound = true;
				break;
			}
			
			// Line is too long
			// Try again with position = position + line offset,
			// i.e. ignore line and go to next one
			// TODO: Shouldn't it be LOG.error instead ??
			log.info("Skipped line of size " +
					newSize + " at pos "
					+ (pos - newSize));
			keyValFound = false;
		}
		
		if (!keyValFound) {
			// We've reached end of Split
			key = null;
			value = null;
		}
		// Tell Hadoop a new line has been found
		// key / value will be retrieved by
		// getCurrentKey getCurrentValue methods
		return keyValFound;
	}
	
	/**
	 * Build the jsonObject, paying attention to opening and closing braces. If not a complete object, keep reading,
	 * otherwise tell hadoop that we have a complete line.
	 * 
	 * @param jsonString
	 *            Text object read from split
	 */
	private boolean addToJsonObj() {
		
		boolean complete = false;
		
		level += StringUtils.countMatches(value.toString(), START_OBJ);
		
		jsonObject.append(value.toString());
		
		log.debug(MARK + " jsonObject = " + jsonObject.toString());
		
		level -= StringUtils.countMatches(value.toString(), END_OBJ);
		
		complete = (level == 0);
		
		if (complete) {
			value.set(jsonObject.toString());
			clearJson();
		}
		
		return complete;
	}
	
	/**
	 * From Design Pattern, O'Reilly... Like the corresponding method of the InputFormat class, this is an optional
	 * method used by the framework for metrics gathering.
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		}
		else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}
	
	/**
	 * From Design Pattern, O'Reilly... This method is used by the framework for cleanup after there are no more
	 * key/value pairs to process.
	 */
	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
	
	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	
	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	
	private void clearJson() {
		clearStrBuilderj(jsonObject);
	}
	
	private void clearStrBuilderj(StringBuilder toClear) {
		if (toClear.length() > 0) {
			toClear.delete(0, toClear.length() - 1);
		}
	}
	
}
