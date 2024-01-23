import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class EncodingText {

    public static class EncodingTextMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        private LongWritable outputKey = new LongWritable();
        private IntWritable outputValue = new IntWritable();
    
        // HashMap para almacenar el diccionario antes de procesar las palabras
        private java.util.Map<String, Integer> dictionary = new java.util.HashMap<>();
        
        // Método que se ejecuta antes de procesar los datos
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Obtener la ruta del diccionario desde el sistema de archivos distribuido
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path dictionaryFilePath = new Path(conf.get("dictionary_path"));

            // Contador para asignar el código numérico a cada palabra	
            Integer counter = 0;
            
            // Abrir el archivo del diccionario y leerlo línea por línea
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

            // Contador para la posición de la palabra en cada línea
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

    // Clase reducer, recibe como entrada la posición de la palabra en el archivo y el código asociado a la palabra
    public static class EncodingTextReducer extends Reducer<LongWritable, IntWritable, NullWritable, IntWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           
            for (IntWritable val : values) {
                
                // La clave de salida es nula, el valor es el código asociado a la palabra
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Uso: EncodingText.jar <ruta_archivo_texto> <ruta_archivo_diccionario> <ruta_salida>");
            System.exit(-1);
        }

        // Configuración del trabajo MapReduce
        Configuration conf = new Configuration();
        conf.set("dictionary_path", args[1]);  // Configurar la ruta del archivo del diccionario

        Job job = Job.getInstance(conf, "Encoding Text Job");
        
        job.setJarByClass(EncodingText.class);
        job.setMapperClass(EncodingTextMapper.class);
        job.setReducerClass(EncodingTextReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Clase creada para escribir en un archivo binario
        job.setOutputFormatClass(FixedBinaryOutputFormat.class);

        // Configuración de las entradas y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Archivo de palabras
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Directorio de salida

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
