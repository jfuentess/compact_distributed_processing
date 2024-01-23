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

import javax.naming.Context;

public class VariableEncodingText {

    public static class VariableEncodingTextMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        private LongWritable outputKey = new LongWritable();
        private IntWritable outputValue = new IntWritable();
    
        // HashMap para almacenar el diccionario antes de procesar las palabras
        private java.util.Map<String, Integer> dictionary = new java.util.HashMap<>();
    
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Obtener la ruta del diccionario desde el sistema de archivos distribuido
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path dictionaryFilePath = new Path(conf.get("dictionary_path"));

            // Contador para asignar el código numérico a cada palabra	
            Integer counter = 0;
            
            // Abrir el diccionario y leerlo línea por línea
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dictionaryFilePath)))) {
                String line;
                
                while ((line = reader.readLine()) != null) {
                    
                    // Obtener la plabra
                    String[] tokens = line.split("\\s+");
                    String word = tokens[0];
                    
                    // Agregar la palabra y el código al map
                    dictionary.put(word, counter++);
                }
            }
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            long counter = 0;
            
            // Obtiene el split actual de la tarea de map
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            // Obtiene el inicio del split actual
            long start = fileSplit.getStart();
            
            // Separar la línea de texto en palabras
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                
                // Obtener la palabra
                String word = new String(itr.nextToken());

                // Obtener el código asociado a la palabra
                Integer number = Integer.valueOf(dictionary.get(word));

                if (number != null) {
                    // La clave de salida es la posición de la palabra en el archivo
                    outputKey.set(start + key.get() + counter);

                    // El valor de salida es el código asociado a la palabra
                    outputValue.set(number);
                    context.write(outputKey, outputValue);

                    counter++;
                } 
            }
        }
    }

    public static class VariableEncodingTextReducer extends Reducer<LongWritable, IntWritable, NullWritable, BytesWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           
            for (IntWritable val : values) {

                BytesWritable bytesValue = getVByteEncode(val.get());

                context.write(NullWritable.get(), bytesValue);
            }
        }

        // Método para convertir un número del tipo int a un arreglo de bytes
        public BytesWritable getVByteEncode(int value){
            
            BytesWritable bytes = new BytesWritable();

            // Tamaño máximo necesario para codificar 4 bytes
            byte[] encodedBytes = new byte[5];

            int i = 0;
            
            // Codifica el int usando VByte
            while(value > 127) {

                encodedBytes[i++] = (byte)(value & 127);
                value>>>=7;
            }

            encodedBytes[i++] = (byte)(value|0x80);
            byte[] result = new byte[i];
            
            System.arraycopy(encodedBytes, 0, result, 0, i);

            // Imprime result como hex
            for (int j = 0; j < result.length; j++) {
                System.out.printf("%02X ", result[j]);
            }

            bytes.set(result, 0, result.length);

            return bytes;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Uso: VariableEncodingText.jar <ruta_archivo_texto> <ruta_archivo_diccionario> <ruta_salida>");
            System.exit(-1);
        }

        // Configuración del trabajo MapReduce
        Configuration conf = new Configuration();
        conf.set("dictionary_path", args[1]);  // Configurar la ruta del archivo del diccionario

        Job job = Job.getInstance(conf, "Variable Encoding Text Job");
        
        job.setJarByClass(VariableEncodingText.class);
        job.setMapperClass(VariableEncodingTextMapper.class);
        job.setReducerClass(VariableEncodingTextReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setOutputFormatClass(VariableBinaryOutputFormat.class);

        // Configuración de las entradas y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Archivo de palabras
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Directorio de salida

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
