package TMP;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
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
import TMP.CustomInputFormat.CustomRecordReader;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import javax.naming.Context;

import java.util.HashMap;
import java.util.Map;

public class TestingMultiMap {

    public static class TestingMultiMapMapper extends Mapper<LongWritable, BytesWritable, NullWritable, Text> {

        private HashMap<Integer, String> dictionary;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            try {

                Configuration conf = context.getConfiguration();
                InputSplit inputSplit = context.getInputSplit();
                RecordReader<LongWritable, BytesWritable> reader = new CustomRecordReader();
                reader.initialize(inputSplit, context);

                dictionary = ((CustomRecordReader) reader).getDictionary();

            }
            catch (IOException e) {
                throw new IOException("Error reading dictionary");
            }
        }
        
        @Override
        public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

            Integer codedWord = decodeVByte(value);
            String word = dictionary.get(codedWord);
            context.write(NullWritable.get(), new Text(word));
        }

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
    }

   // Clases de comparador y particionador ficticias que no hacen nada
    public static class NoSortComparator extends WritableComparator {
        public NoSortComparator() {
            super(NullWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // No hace ninguna comparación
            return 0;
        }
    }

    public static class NoPartitioner extends Partitioner<NullWritable, Text> {
        @Override
        public int getPartition(NullWritable key, Text value, int numPartitions) {
            // No asigna ninguna partición
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
       
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TestingMultiMap");
        
        job.setJarByClass(TestingMultiMap.class);
        job.setMapperClass(TestingMultiMapMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(CustomInputFormat.class);
        job.setOutputFormatClass(CustomOutputFormat.class);

        job.setSortComparatorClass(NoSortComparator.class);
        job.setPartitionerClass(NoPartitioner.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
