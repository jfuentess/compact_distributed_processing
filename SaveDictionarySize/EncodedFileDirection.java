import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.DataOutputStream;

// Save the location in bytes of the encoded file, after the dictionary
public class EncodedFileDirection {

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.err.println("Use: java EncodedFileDirection <InputDictionary> <OutputFile>");
            System.exit(1);
        }

        // Get the input file name
        String inputFileName = args[0];

        // Get the size of the input file
        File inputFile= new File(inputFileName);
        long size = inputFile.length();

        // Validate the size of the input file
        if (size > Integer.MAX_VALUE) {
            System.err.println("The file is too large");
            System.exit(1);
        }

        // Convert the size to an integer and add 4 (to include the dictionary size)
        int sizeInt = (int) size + 4;

        // Create the output file
        String outputFileName = args[1];
        FileOutputStream outputFile = new FileOutputStream(outputFileName);

        // Create the data output stream
        DataOutputStream dataOutputStream = new DataOutputStream(outputFile);

        // Write the size to the output file
        dataOutputStream.writeInt(sizeInt);

        // Close the output file
        dataOutputStream.close();
        outputFile.close();
    }
}
