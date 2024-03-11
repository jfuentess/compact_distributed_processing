package VariableEncodingGlobal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;

public class VEGInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    // Local dictionary of each file
    private static HashMap<Integer, String> dictionary = new HashMap<Integer, String>();

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream inputStream = fs.open(path);

        inputStream.seek(0);

        // Read the first 4 bytes to get the location of the encoded file
        byte[] encodedFileDirectionBytes = new byte[4];
        inputStream.read(encodedFileDirectionBytes);

        int encodedFileDirection = ByteBuffer.wrap(encodedFileDirectionBytes).getInt();

        // Read the dictionary from the file
        byte[] dictionaryBytes = new byte[encodedFileDirection - 4];

        inputStream.read(4, dictionaryBytes, 0, encodedFileDirection - 4);

        String dictionaryString = new String(dictionaryBytes);

        // Load the dictionary into a HashMap
        HashMap<Integer, String> dictionary = loadDictionary(dictionaryString);

        return new VEGRecordReader();
    }

    // Load the dictionary into a HashMap
    private HashMap<Integer, String> loadDictionary(String dictionaryString) {
            
        // Read the string line by line
        BufferedReader bf = new BufferedReader(new StringReader(dictionaryString));

        String line;
        int counter = 0;

        try {
            while ((line = bf.readLine()) != null) {
                
                String[] tokens = line.split("\\s+");
                dictionary.put(counter++, tokens[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dictionary;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        
        return true;
    }

    @Override
    public List <InputSplit> getSplits(JobContext context) throws IOException {

        Configuration conf = context.getConfiguration();
        List<FileStatus> files = listStatus(context);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // There is just one file
        for (FileStatus file: files) {
            // Get the file path
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(conf);
            // Get the file block locations
            BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
            // List to store the record boundaries
            List<Long> recordBoundaries = new ArrayList<Long>();
            // Open the file
            FSDataInputStream inStream = fs.open(path);

            // Read the first 4 bytes to get the location of the encoded file
            byte[] encodedFileDirectionBytes = new byte[4];
            inStream.read(encodedFileDirectionBytes);

            int encodedFileDirection = ByteBuffer.wrap(encodedFileDirectionBytes).getInt();

            recordBoundaries.add((long)encodedFileDirection);

            // For each block
            for (int i = 0; i < blockLocations.length - 1; i++) {
                long start = blockLocations[i+1].getOffset();
                inStream.seek(start);
                while (true) {
                    // Read a byte
                    int b = inStream.read();

                    // If the most significant bit is 1, the record boundary has been found
                    if ((b & 0x80) != 0) {
                        // Add the position of the record boundary
                        recordBoundaries.add(inStream.getPos());
                        break;
                    }
                }       
            }

            inStream.close();

            long lastBoundarie = (long)(blockLocations[blockLocations.length - 1].getOffset() + blockLocations[blockLocations.length - 1].getLength());
            if (!recordBoundaries.contains(lastBoundarie)) recordBoundaries.add(lastBoundarie);

            for (int i = 0; i < recordBoundaries.size() - 1; i++) {

                long start = recordBoundaries.get(i);
                long end = recordBoundaries.get(i + 1);
                splits.add(new FileSplit(path, start, end - start, null));
            }
        }

        return splits;
    }

    // Custom RecordReader
    public static class VEGRecordReader extends RecordReader<LongWritable, BytesWritable> {

        // InputSplit
        private FSDataInputStream in;
        // Start position of the split
        private long start;
        // End position of the split
        private long end;
        // Current position of the split
        private long pos;

        // Key-value pair
        private LongWritable key = new LongWritable();
        private BytesWritable value = new BytesWritable();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) split;
            Configuration conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);

            // Open the file
            in = fs.open(path);
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            pos = start;
            // Move the pointer to the start position
            in.seek(start);

            System.out.println("Start: " + start);

        }

        // Get the dictionary
        public HashMap<Integer, String> getDictionary() {
            return dictionary;
        }

        // Get the next key-value pair
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            
            // Read the value from the binary file
            // Each value ends with a byte with the the leftmost bit set to 1
            if (pos < end) {
                int length = 0;
                byte[] bytes = new byte[5];

                while (true) {
                    
                    ByteBuffer buffer = ByteBuffer.allocate(1);
                   
                    in.readFully(buffer.array());

                    byte b = buffer.get();

                    bytes[length] = b;
                    length++;

                    if ((b & 0x80) != 0) {
                        break;
                    }
                    if (length == 5) {
                        throw new IOException("Error: Record too long");
                    }
                }

                value.set(bytes, 0, length);
                key.set(pos);
                pos = in.getPos();

                return true;
            }

            return false;
        }

        // The key is the position of the encoded word in the file
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {

            return key;
        }

        // The value is the VByte encoded word
        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            
            return (float)(pos - start) / (end - start);
        }

        @Override
        public void close() throws IOException {
            
            in.close();
        }
    }
}
