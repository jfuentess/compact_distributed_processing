package VEMF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import VEMF.VEMFInputFormat.VEMFRecordReader;

// Read multiple encoded files and reencode the words using a global dictionary
public class VariableEncodingMultipleFiles {

    public static class VEMFMapper extends Mapper<LongWritable, BytesWritable, NullWritable, BytesWritable> {

        private HashMap<Integer, String> localDictionary;
        private HashMap<String, Integer> globalDictionary = new HashMap<String, Integer>();
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            try {

                // Get the local dictionary from the RecordReader
                InputSplit inputSplit = context.getInputSplit();
                RecordReader<LongWritable, BytesWritable> reader = new VEMFRecordReader();
                reader.initialize(inputSplit, context);
                localDictionary = ((VEMFRecordReader) reader).getDictionary();

                // Get the global dictionary
                Configuration conf = context.getConfiguration();
                FileSystem fs = FileSystem.get(conf);
                Path globalDictionaryFilePath = new Path(conf.get("global_dictionary"));

                // Counter for the global dictionary
                Integer counter = 0;

                // Read the global dictionary
                try (BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(globalDictionaryFilePath)))) {

                    // Create the global dictionary into a HashMap
                    String line;
                    while ((line = bf.readLine()) != null) {
                        String[] tokens = line.split("\\s+");
                        String word = tokens[0];
                        globalDictionary.put(word, counter++);
                    }
                }
                    
                catch (IOException e) {
                    throw new IOException("Error reading global dictionary");
                }

                        
            }
            catch (IOException e) {
                throw new IOException("Error reading local dictionary");
            }
        }
        
        // Map method: decode the word using the local dictionary and encode it using the global dictionary
        @Override
        public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

            // Decode the word using the local dictionary
            Integer localCode = decodeVByte(value);    
            String localWord = localDictionary.get(localCode);

            // Encode the word using the global dictionary
            Integer globalCode = globalDictionary.get(localWord);
            BytesWritable globalCodeBytes = encodeVByte(globalCode.intValue());

            // Write the encoded word to the output
            context.write(NullWritable.get(), globalCodeBytes);
        }

        // Decode an integer using VByte
        private Integer decodeVByte(BytesWritable valueBytes){
            
            int number = 0;
            byte[] bytes = valueBytes.getBytes();

            for (int i = 0; i < bytes.length; i++) {
                number |= (bytes[i] & 0x7F) << (7 * i);

                if ((bytes[i] & 0x80) != 0) {
                    break;
                }
            }

            return Integer.valueOf(number);
        }

        // Encode an integer using VByte
        private BytesWritable encodeVByte(int value) {
            
            BytesWritable bytes = new BytesWritable();
            byte[] encodedBytes = new byte[5];
            int i = 0;

            while(value > 127){

                encodedBytes[i++] = (byte)(value & 127);
                value >>>=7;
            }

            encodedBytes[i++] = (byte)(value | 0x80);
            byte[] result = new byte[i];

            System.arraycopy(encodedBytes, 0, result, 0, i);
            bytes.set(result, 0, result.length);

            return bytes;
        }

    }

    // There is no need for a comparator or a partitioner
    public static class NoSortComparator extends WritableComparator {
        public NoSortComparator() {
            super(NullWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return 0;
        }
    }

    public static class NoPartitioner extends Partitioner<NullWritable, Text> {
        @Override
        public int getPartition(NullWritable key, Text value, int numPartitions) {
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: yarn jar VariableEncodingMultipleFiles.jar VEMF.VariableEncodingMultipleFiles <input_path> <global_dictionary> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("global_dictionary", args[1]); 

        Job job = Job.getInstance(conf, "VariableEncodingMultipleFiles");
        
        job.setJarByClass(VariableEncodingMultipleFiles.class);
        job.setMapperClass(VEMFMapper.class);
        // There is no reducer
        job.setNumReduceTasks(0);

        job.setInputFormatClass(VEMFInputFormat.class);
        job.setOutputFormatClass(VEMFOutputFormat.class);

        job.setSortComparatorClass(NoSortComparator.class);
        job.setPartitionerClass(NoPartitioner.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
