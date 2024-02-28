package TMP;

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
import java.io.BufferedReader;
import java.nio.ByteBuffer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        return new CustomRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        
        return false;
    }

    public static class CustomRecordReader extends RecordReader<LongWritable, BytesWritable> {

        private FSDataInputStream in;
        private long start;
        private long end;
        private long pos;
        private HashMap<Integer, String> dictionary = new HashMap<Integer, String>();
        private LongWritable key = new LongWritable();
        private BytesWritable value = new BytesWritable();

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) split;
            Configuration conf = context.getConfiguration();
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);

            in = fs.open(path);
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            pos = start;
            in.seek(start);

            byte[] encodedFileDirectionBytes = new byte[4];
            in.read(encodedFileDirectionBytes);
            int encodedFileDirection = ByteBuffer.wrap(encodedFileDirectionBytes).getInt();

            byte[] bytesDictionary = new byte[encodedFileDirection - 4];
            in.read(bytesDictionary);
            String dictionaryString = new String(bytesDictionary);

            dictionary = loadDictionary(dictionaryString);
        }

        private HashMap<Integer, String> loadDictionary(String dictionaryString) {
            
            String[] lines = dictionaryString.split("\n");
            for (int i = 0; i < lines.length; i++) {
                String[] tokens = lines[i].split("\\s+");
                dictionary.put(i, tokens[0]);
            }
            return dictionary;
        }

        public HashMap<Integer, String> getDictionary() {
            return dictionary;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
                
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

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {

            return key;
        }

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
