import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.HashMap;

// Class to encode a text file using a dictionary and VByte encoding
public class VariableEncodingText {

    // Mapper class
    public static class VariableEncodingTextMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        private LongWritable outputKey = new LongWritable();
        private IntWritable outputValue = new IntWritable();
    
        // HashMap to store the dictionary
        private HashMap<String, Integer> dictionary = new HashMap<String, Integer>();
    
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            // Get the dictionary file path
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path dictionaryFilePath = new Path(conf.get("dictionary_path"));

            // Counter to store the code of each word
            Integer counter = 0;
            
            // Open the dictionary file and read it line by line
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dictionaryFilePath)))) {
                String line;
                
                while ((line = reader.readLine()) != null) {
                    
                    // Get the word
                    String[] tokens = line.split("\\s+");
                    String word = tokens[0];
                    
                    // Add the word and its code to the dictionary
                    dictionary.put(word, counter++);
                }
            }
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Position of the word in the line
            long counter = 0;
            
            // Get the start position of the input split
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            long start = fileSplit.getStart();
            
            // Tokenize the input line
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                
                // Get the code of the word
                String word = new String(itr.nextToken());
                Integer number = Integer.valueOf(dictionary.get(word));

                if (number != null) {
                    
                    // Set the unique output key
                    outputKey.set(start + key.get() + counter);

                    // Set the output value
                    outputValue.set(number);
                    context.write(outputKey, outputValue);

                    counter++;
                } 
            }
        }
    }

    // Reducer class
    public static class VariableEncodingTextReducer extends Reducer<LongWritable, IntWritable, NullWritable, BytesWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           
            // There is just one value
            for (IntWritable val : values) {

                // Encode the value using VByte encoding
                BytesWritable bytesValue = getVByteEncode(val.get());
                
                // Write the encoded value to the context. No key is needed
                context.write(NullWritable.get(), bytesValue);
            }
        }

        // Function to encode an integer using VByte
        public BytesWritable getVByteEncode(int value){
            
            BytesWritable bytes = new BytesWritable();

            // Array to store the encoded bytes
            byte[] encodedBytes = new byte[5];

            int i = 0;
            
            // Encode the integer using VByte encoding
            while(value > 127) {

                encodedBytes[i++] = (byte)(value & 127);
                value>>>= 7;
            }

            encodedBytes[i++] = (byte)(value | 0x80);
            byte[] result = new byte[i];
            
            System.arraycopy(encodedBytes, 0, result, 0, i);
            bytes.set(result, 0, result.length);

            return bytes;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Use: VariableEncodingText.jar <input_directory_path> <input_dictionary_file_path> <output_directory>");
            System.exit(-1);
        }

        // MapReduce job configuration
        Configuration conf = new Configuration();
        // Set the dictionary file path
        conf.set("dictionary_path", args[1]);

        Job job = Job.getInstance(conf, "Variable Encoding Text Job");
        
        job.setJarByClass(VariableEncodingText.class);
        job.setMapperClass(VariableEncodingTextMapper.class);
        job.setReducerClass(VariableEncodingTextReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setOutputFormatClass(VariableBinaryOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[2])); 

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
