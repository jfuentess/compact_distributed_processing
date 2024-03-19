#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Use: $0 <directory>"
    exit 1
fi

directory="$1"

if [ ! -d "$directory" ]; then
    echo "The directory $directory does not exist"
    exit 1
fi

count_file=0

for file in "$directory"/*; do
    if [ -f "$file" ]; then

        echo -e "\n$count_file: $file\n"
        
        # Add the file to the HDFS
        hadoop fs -put $file /user/pi/tests/input

        echo -e "\nFile $file added to the HDFS\nWordCount:\n"

        # Run the WordCount program
        yarn jar /home/pi/WordCount/WordCount.jar WordCount /user/pi/tests/input /user/pi/tests/output
        
        # Get the output from the HDFS
        hadoop fs -get /user/pi/tests/output/part-r-00000 /home/pi/tests/tmp/output

        # Sort the output to create the dictionary
        sort -k2,2nr /home/pi/tests/tmp/output > /home/pi/tests/dictionaries/dict_$count_file

        # Remove the output from the HDFS
        hadoop fs -rm -r /user/pi/tests/output

        # Insert the dictionary into the HDFS
        hadoop fs -put /home/pi/tests/dictionaries/dict_$count_file /user/pi/tests/dictionary
        
        echo -e "\nDictionary dict_$count_file created\nEncoding file:\n"
        # Encode the input text with the dictionary
        yarn jar /home/pi/VariableEncodingText/VariableEncodingText.jar VariableEncodingText /user/pi/tests/input /user/pi/tests/dictionary /user/pi/tests/output

        # Get the output from the HDFS
        hadoop fs -get /user/pi/tests/output/binary_output /home/pi/tests/tmp/binary_output_$count_file

        # Create the header from the dictionary
        java -cp /home/pi/EncodeFileDirection/ EncodeFileDirection /home/pi/tests/dictionaries/dict_$count_file /home/pi/tests/tmp/header_$count_file

        # Concatenate the header, the dictionary and the binary output
        cat /home/pi/tests/tmp/header_$count_file /home/pi/tests/dictionaries/dict_$count_file /home/pi/tests/tmp/binary_output_$count_file > /home/pi/tests/encoded_files/encoded_file_$count_file

        echo -e "\nFile encoded_file_$count_file created\n"

        # Remove the temporary files
        rm /home/pi/tests/tmp/*

        # Remove the files from the HDFS
        hadoop fs -rm /user/pi/tests/input/*
        hadoop fs -rm -r /user/pi/tests/output
        hadoop fs -rm /user/pi/tests/dictionary

        ((count_file++))

    fi
done

echo -e "\nAll files encoded, Phase 1 finished\n"

# Create global dictionary
java -cp /home/pi/DictionaryUnification DictionaryUnification /home/pi/tests/dictionaries /home/pi/tests/dictionaries/temp_global_dict

# Sort the global dictionary
sort -k2,2nr /home/pi/tests/dictionaries/temp_global_dict > /home/pi/tests/dictionaries/global_dict

# Remove the temporary dictionary
rm /home/pi/tests/dictionaries/temp_global_dict

# Insert the global dictionary into the HDFS
hadoop fs -put /home/pi/tests/dictionaries/global_dict /user/pi/tests/dictionary

echo -e "\nGlobal dictionary created, Phase 2 finished\n"

# Insert the encoded files into the HDFS

hadoop fs -put /home/pi/tests/encoded_files/* /user/pi/tests/input

echo -e "\nRe-encoding files\n"

# Run the re-encoding program
yarn jar VariableEncodingMultipleFiles/VariableEncodingMultipleFiles.jar VEMF.VariableEncodingMultipleFiles /user/pi/tests/input /user/pi/tests/dictionary /user/pi/tests/output

# Get the output files from the HDFS
hadoop fs -get /user/pi/tests/output/* /home/pi/tests/phase3_files

# Remove the files from the HDFS
hadoop fs -rm /user/pi/tests/input/*
hadoop fs -rm -r /user/pi/tests/output
hadoop fs -rm /user/pi/tests/dictionary

# Remove _SUCCESS file
rm /home/pi/tests/phase3_files/_SUCCESS

echo -e "\nFiles re-encoded, Phase 3 finished\n"