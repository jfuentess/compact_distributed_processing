package VariableEncodingMultipleFiles;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.BufferedReader;
import java.nio.ByteBuffer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VEMFInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        return new VEMFRecordReader();
    }

    // Each file is a split
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        
        return false;
    }

    // Custom RecordReader
    public static class VEMFRecordReader extends RecordReader<LongWritable, BytesWritable> {

        // InputSplit
        private FSDataInputStream in;
        // Start position of the split
        private long start;
        // End position of the split
        private long end;
        // Current position of the split
        private long pos;
        // Local dictionary of each file
        private HashMap<Integer, String> dictionary = new HashMap<Integer, String>();

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

            // Read the first 4 bytes to get the location of the encoded file
            byte[] encodedFileDirectionBytes = new byte[4];
            in.read(encodedFileDirectionBytes);
            int encodedFileDirection = ByteBuffer.wrap(encodedFileDirectionBytes).getInt();

            // Read the dictionary from the file
            byte[] bytesDictionary = new byte[encodedFileDirection - 4];
            in.read(4, bytesDictionary, 0, encodedFileDirection - 4);
            String dictionaryString = new String(bytesDictionary);

            // Load the dictionary into a HashMap
            dictionary = loadDictionary(dictionaryString);

            in.seek(encodedFileDirection);
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
                    int b = in.read();
                    bytes[length] = (byte)b;
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
