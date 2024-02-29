import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.DataOutputStream;

// Class to define the output format for the variable-length binary format
public class VariableBinaryOutputFormat extends FileOutputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordWriter<NullWritable, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new VariableBinaryRecordWriter(taskAttemptContext);
    }
}

// RecordWriter class to write the variable-length binary records
class VariableBinaryRecordWriter extends RecordWriter<NullWritable, BytesWritable> {
    
    private DataOutputStream outputStream;

    public VariableBinaryRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {

        // Output path
        Path outputPath = FileOutputFormat.getOutputPath(taskAttemptContext);

        // Name of the output file
        String outputFileName = "binary_output";

        // Set the path of the output file
        Path outputFile = new Path(outputPath, outputFileName);

        // Get the DataOutputStream to write the output file
        FSDataOutputStream fsOutput = outputPath.getFileSystem(taskAttemptContext.getConfiguration()).create(outputFile, true);

        this.outputStream = new DataOutputStream(fsOutput);
    }

    // Write method to write the variable-length binary records
    @Override
    public void write(NullWritable key, BytesWritable value) throws IOException, InterruptedException {

        // Get byte array from the value
        byte[] bytes = value.copyBytes();

        // Write the byte array to the output file
        outputStream.write(bytes);
    }
    
    // Method to close the output stream
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outputStream.close();
    }
}
