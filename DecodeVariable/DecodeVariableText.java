import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DecodeVariableText{

    public static void main(String[] args) throws Exception{

        String encodedText = args[0];

        String dictionaryArg = args[1];

        Map<Integer, String> dictionary = new HashMap<Integer, String>();
    
        try{
            
            // Lee el diccionario y lo almacena en un HashMap
            Integer counter = 0;

            FileInputStream fstream = new FileInputStream(dictionaryArg);

            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;

            while ((strLine = br.readLine()) != null)   {
                String[] tokens = strLine.split("\\s+");
                dictionary.put(counter++, tokens[0]);
            }

            br.close();

            // Lee el archivo binario y lo decodifica como número entero
            FileInputStream fstreamBinary = new FileInputStream(encodedText);

            DataInputStream disBytes = new DataInputStream(fstreamBinary);

            // Arreglo para guardar los bytes decodificados leídos del archivo binario
            ArrayList<Integer> dataVBytes = new ArrayList<Integer>();

            // Lee los bytes del archivo binario
            while(disBytes.available() > 0){

                int j = 0;
                // Lee bytes hasta encontrar un byte con el bit más significativo en 1
                byte[] bytes = new byte[5];
                
                byte b = disBytes.readByte();
                
                while((b & 0x80) == 0){
                    bytes[j] = b;
                    b = disBytes.readByte();
                    j++;
                }

                bytes[j] = b;

                // Decodifica el byte y lo guarda en el arreglo
                dataVBytes.add(decode(bytes));   
            }

            fstreamBinary.close();
            disBytes.close(); 
            
            int c = 1;
            // Decodifica los números enteros usando el diccionario
            BufferedWriter bw = new BufferedWriter(new FileWriter(encodedText + "_decoded.txt"));

            for(Integer i : dataVBytes){

                if(c % 10 == 0)
                    bw.write(dictionary.get(i) + "\n");
                else
                    bw.write(dictionary.get(i) + " ");
                
                c++;
            }

            bw.flush();
            bw.close();
        }
        catch(Exception e){
            System.out.println("Error: " + e.getMessage());
        }
    }
    // Decodifica un arreglo de bytes usando VByte
    public static int decode(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value |= (bytes[i] & 127) << (7 * i);
            if ((bytes[i] & 128) != 0) {
                return value;
            }
        }
        throw new IllegalArgumentException("bytes is not a valid Vbyte encoding");
    }
}