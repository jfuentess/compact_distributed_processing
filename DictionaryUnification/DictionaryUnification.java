import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// This program reads a folder of files, each containing a dictionary of words and their frequency, and unifies them into a single dictionary.
public class DictionaryUnification {
    
    public static void main(String[] args) throws IOException {
        
        // Check if the number of arguments is correct
        if (args.length != 2) {
            System.out.println("Usage: java DictionaryUnification <folderPath> <unifiedDictionaryFile>");
            System.exit(1);
        }

        String folderPath = args[0];
        String unifiedDictionaryFile = args[1];

        // Create a list of files in the folder
        File folder = new File(folderPath);
        File[] listOfFiles = folder.listFiles();

        // Create the unified dictionary
        HashMap<String, Integer> unifiedDictionary = new HashMap<String, Integer>();

        // Read each file in the folder
        for (File file : listOfFiles) {
            if (file.isFile()) {
                // Read the file
                try {
                    BufferedReader br = new BufferedReader(new FileReader(file));

                    // Read each line in the file
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Split the line into tokens
                        String[] tokens = line.split("\\s+");
                        
                        // If the token is in the unified dictionary, increment its count
                        if (unifiedDictionary.containsKey(tokens[0])) {
                            unifiedDictionary.put(tokens[0], unifiedDictionary.get(tokens[0]) + Integer.parseInt(tokens[1]));
                        }
                        // Otherwise, add the token to the unified dictionary
                        else
                            unifiedDictionary.put(tokens[0], Integer.parseInt(tokens[1]));
                    }

                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        // Write the unified dictionary to a new file
        BufferedWriter bw = new BufferedWriter(new FileWriter(unifiedDictionaryFile));

        for (Map.Entry<String, Integer> entry : unifiedDictionary.entrySet()) {
            bw.write(entry.getKey() + " " + entry.getValue() + "\n");
        }

        bw.close();        
    }
}
