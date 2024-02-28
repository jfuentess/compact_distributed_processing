package TMP;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomOutputFormat extends FileOutputFormat<NullWritable, Text> {

    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        Path outputDir = FileOutputFormat.getOutputPath(taskAttemptContext);
        Path outputFile = new Path(outputDir, "output_" + taskAttemptContext.getTaskAttemptID().getTaskID().getId());

        FileSystem fs = outputDir.getFileSystem(taskAttemptContext.getConfiguration());
        FSDataOutputStream fsOutput = fs.create(outputFile, true);

        return new CustomRecordWriter(fsOutput);
    }

    private static class CustomRecordWriter extends RecordWriter<NullWritable, Text> {

        private final FSDataOutputStream fsOutput;

        public CustomRecordWriter(FSDataOutputStream fsOutput) {
            this.fsOutput = fsOutput;
        }

        @Override
        public void write(NullWritable key, Text value) throws IOException, InterruptedException {

            fsOutput.write(value.getBytes(), 0, value.getLength());
            fsOutput.write('\n');
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            fsOutput.close();
        }
    }
}
