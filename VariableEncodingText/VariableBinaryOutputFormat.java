import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.DataOutputStream;

// Clase que define el formato de salida binario de largo variable
public class VariableBinaryOutputFormat extends FileOutputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordWriter<NullWritable, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new VariableBinaryRecordWriter(taskAttemptContext);
    }
}

// Clase que define el record writer para el formato de salida binario de largo variable
class VariableBinaryRecordWriter extends RecordWriter<NullWritable, BytesWritable> {
    
    private DataOutputStream outputStream;

    public VariableBinaryRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {

        // Drectorio de salida del trabajo MapReduce
        Path outputPath = FileOutputFormat.getOutputPath(taskAttemptContext);

        // Nombre del archivo de salida
        String outputFileName = "binary_output";

        // Combinación el directorio de salida y el nombre del archivo
        Path outputFile = new Path(outputPath, outputFileName);

        // Obtener un FSDataOutputStream para el archivo de salida
        FSDataOutputStream fsOutput = outputPath.getFileSystem(taskAttemptContext.getConfiguration()).create(outputFile, true);

        this.outputStream = new DataOutputStream(fsOutput);
    }

    // Método que escribe un registro en el archivo de salida
    @Override
    public void write(NullWritable key, BytesWritable value) throws IOException, InterruptedException {

        // Obtiene el arreglo de bytes del valor
        byte[] bytes = value.copyBytes();

        // Escribe el valor codificado en el archivo de salida como un arreglo de bytes
        outputStream.write(bytes);
    }
    
    // Método que cierra el archivo de salida
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outputStream.close();
    }
}
