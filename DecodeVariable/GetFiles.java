import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GetFiles {

    public static void main(String[] args) throws IOException {
        // Ruta del archivo
        String inputFile = args[0];

        // Abrir el archivo en modo lectura
        try (FileInputStream fis = new FileInputStream(inputFile)) {

            // Leer los primeros 4 bytes para obtener el tamaño del archivo de texto
            byte[] encodedFileDirectionBytes = new byte[4];
            fis.read(encodedFileDirectionBytes);
            int encodedFileDirection = ByteBuffer.wrap(encodedFileDirectionBytes).getInt();

            // Leer el archivo de texto
            byte[] bytesDictionary = new byte[encodedFileDirection - 4];
            fis.read(bytesDictionary);
            String dictionary = new String(bytesDictionary);

            // Leer el archivo binario
            byte[] bytesEncodedFile = new byte[fis.available()];
            fis.read(bytesEncodedFile);

            // Write the dictionary to a new file
            FileOutputStream fos = new FileOutputStream("dictionary.txt");
            fos.write(dictionary.getBytes());
            fos.close();

            // Write the encoded file to a new file
            FileOutputStream fos2 = new FileOutputStream("encodedFile.bin");
            fos2.write(bytesEncodedFile);
            fos2.close();            

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
