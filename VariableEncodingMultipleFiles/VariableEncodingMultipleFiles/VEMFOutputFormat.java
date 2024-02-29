package VariableEncodingMultipleFiles;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.DataOutputStream;

// Custom OutputFormat
public class VEMFOutputFormat extends FileOutputFormat<NullWritable, BytesWritable> {

    @Override
    public RecordWriter<NullWritable, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        // Each map generates an output file
        Path outputDir = FileOutputFormat.getOutputPath(taskAttemptContext);
        Path outputFile = new Path(outputDir, "output_" + taskAttemptContext.getTaskAttemptID().getTaskID().getId());
        FileSystem fs = outputDir.getFileSystem(taskAttemptContext.getConfiguration());
        FSDataOutputStream fsOutput = fs.create(outputFile, true);

        return new VEMFRecordWriter(fsOutput);
    }

    // Custom RecordWriter
    private static class VEMFRecordWriter extends RecordWriter<NullWritable, BytesWritable> {

        private final FSDataOutputStream fsOutput;
        private DataOutputStream outputStream;

        public VEMFRecordWriter(FSDataOutputStream fsOutput) {
            this.fsOutput = fsOutput;
            outputStream = new DataOutputStream(fsOutput);
        }

        // Write the value as byte array to the output file
        @Override
        public void write(NullWritable key, BytesWritable value) throws IOException, InterruptedException {

            byte[] bytes = value.copyBytes();
            outputStream.write(bytes);
        }

        // Close the output file
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            outputStream.close();
            fsOutput.close();
        }
    }
}
