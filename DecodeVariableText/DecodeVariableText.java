import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

// Program to decode a VByte encoded file using a dictionary
public class DecodeVariableText{

    public static void main(String[] args) throws Exception{

        // Check if the number of arguments is correct
        if (args.length != 2) {
            System.out.println("Usage: java DecodeVariableText <encodedText> <dictionary>");
            System.exit(1);
        }

        String encodedText = args[0];

        String dictionaryArg = args[1];

        Map<Integer, String> dictionary = new HashMap<Integer, String>();
    
        try{
            
            // Read the dictionary file and store it in a map
            Integer counter = 0;

            FileInputStream fstream = new FileInputStream(dictionaryArg);

            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;

            while ((strLine = br.readLine()) != null)   {
                String[] tokens = strLine.split("\\s+");
                dictionary.put(counter++, tokens[0]);
            }

            br.close();

            // Read the encoded file
            FileInputStream fstreamBinary = new FileInputStream(encodedText);

            DataInputStream disBytes = new DataInputStream(fstreamBinary);

            // ArrayList to store the decoded integers
            ArrayList<Integer> dataVBytes = new ArrayList<Integer>();

            // Read the file byte by byte
            while(disBytes.available() > 0){

                int j = 0;
                // Read the byte until the more significant bit is 1
                byte[] bytes = new byte[5];
                
                byte b = disBytes.readByte();
                
                while((b & 0x80) == 0){
                    bytes[j] = b;
                    b = disBytes.readByte();
                    j++;
                }

                bytes[j] = b;

                // Decode the bytes and store the integer in the ArrayList
                dataVBytes.add(decode(bytes));   
            }

            fstreamBinary.close();
            disBytes.close(); 
            
            int c = 1;
            // Decode the integers and write the decoded text to a file
            BufferedWriter bw = new BufferedWriter(new FileWriter(encodedText + "_decoded"));

            for(Integer i : dataVBytes){

                // Add a new line every 10 words
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
    // Decode an array of bytes into an integer using VByte encoding
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