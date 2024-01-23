import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.DataOutputStream;

// Clase que define el formato de salida binario de largo fijo
public class FixedBinaryOutputFormat extends FileOutputFormat<NullWritable, IntWritable> {
    @Override
    public RecordWriter<NullWritable, IntWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FixedBinaryRecordWriter(taskAttemptContext);
    }
}

// Clase que define el record writer para el formato de salida binario de largo fijo
class FixedBinaryRecordWriter extends RecordWriter<NullWritable, IntWritable> {
    
    private DataOutputStream outputStream;

    public FixedBinaryRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {

        // Drectorio de salida del trabajo MapReduce
        Path outputPath = FileOutputFormat.getOutputPath(taskAttemptContext);

        // Nombre del archivo de salida
        String outputFileName = "binary_output";

        // Combinación el directorio de salida y el nombre del archivo
        Path outputFile = new Path(outputPath, outputFileName);

        // Obtener un FSDataOutputStream para el archivo de salida
        FSDataOutputStream fsOutput = outputPath.getFileSystem(taskAttemptContext.getConfiguration()).create(outputFile, true);

        // Crear un DataOutputStream para escribir datos binarios
        this.outputStream = new DataOutputStream(fsOutput);
    }

    // Método que escribe un registro en el archivo de salida
    @Override
    public void write(NullWritable key, IntWritable value) throws IOException, InterruptedException {

        // Escribe en el archivo de salida como binario (largo fijo de 4 bytes)
        outputStream.writeInt(value.get());        
    }

    // Método que cierra el archivo de salida
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outputStream.close();
    }
}
