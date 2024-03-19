package VEG;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.io.DataOutputStream;

// Custom OutputFormat
public class VEGOutputFormat extends FileOutputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordWriter<LongWritable, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new VEGRecordWriter(taskAttemptContext);
    }

    // Custom RecordWriter
    private static class VEGRecordWriter extends RecordWriter<LongWritable, BytesWritable> {

        private DataOutputStream outputStream;

        public VEGRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {

            Path outputDir = FileOutputFormat.getOutputPath(taskAttemptContext);
            Path outputFile = new Path(outputDir, "output_" + taskAttemptContext.getTaskAttemptID().getTaskID().getId());
            FileSystem fs = outputDir.getFileSystem(taskAttemptContext.getConfiguration());
            FSDataOutputStream fsOutput = fs.create(outputFile, true);
            outputStream = new DataOutputStream(fsOutput);
        }

        // Write the value as byte array to the output file
        @Override
        public void write(LongWritable key, BytesWritable value) throws IOException, InterruptedException {

            byte[] bytes = value.copyBytes();
            outputStream.write(bytes);
        }

        // Close the output file
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            outputStream.close();
        }
    }
}
