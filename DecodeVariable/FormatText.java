import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class FormatText {

    public static void main(String[] args) {
        // Especifica la ruta del archivo que deseas procesar
        String rutaArchivo = args[0];
        
        // Especifica la ruta del archivo de salida
        String rutaArchivoSalida = args[1];

        try {
            // Crea un lector de archivos
            BufferedReader br = new BufferedReader(new FileReader(rutaArchivo));

            // Crea un escritor de archivos de salida
            BufferedWriter bw = new BufferedWriter(new FileWriter(rutaArchivoSalida));

            // Variable para contar las palabras
            int contadorPalabras = 0;

            // Variable para almacenar las palabras concatenadas
            StringBuilder palabrasConcatenadas = new StringBuilder();

            // Lee cada línea del archivo
            String linea;
            while ((linea = br.readLine()) != null) {
                // Usa StringTokenizer para dividir la línea en palabras
                StringTokenizer tokenizer = new StringTokenizer(linea);

                while (tokenizer.hasMoreTokens()) {
                    // Obtiene la siguiente palabra
                    String palabra = tokenizer.nextToken();

                    // Concatena la palabra al resultado
                    palabrasConcatenadas.append(palabra);

                    // Incrementa el contador de palabras
                    contadorPalabras++;

                    // Agrega un salto de línea después de cada décima palabra
                    if (contadorPalabras % 10 == 0) 
                        palabrasConcatenadas.append("\n");
                    else 
                        palabrasConcatenadas.append(" ");
                }
            }

             // Escribe el resultado en el archivo de salida
             bw.write(palabrasConcatenadas.toString());

             // Cierra los lectores y escritores de archivos
             br.close();
             bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
