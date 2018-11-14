### CSE 512 - Assignment 4 - Map-reduce program that will perform equijoin
## Command to execute

sudo -u <username> <path_of_hadoop> jar <name_of_jar> <class_with_main_function> HDFSinputFile HDFSoutputFile

Ex: sudo -u hduser /usr/local/hadoop/bin/hadoop jar equijoin.jar Equijoin hdfs://localhost:54310/input/sample.txt hdfs://localhost:54310/output

### Make sure you pass 3 arguments after specifying jar (classname, inputFilePath, outputFilePath)

In the submission,
1. Jar name: equijoin.jar
2. classname: Equijoin (In java, classname starts with capital letter.)

## Driver
In driver (main function), a job is created with the name "equijoin". For the job, mapper class, reducer class, type of key-value from mapper output, type of key-value from reducer output (final output), input file and output file are set. Job is started and the driver waits till it completes and then exits.

## Mapper
Mapper gets the entire input file as input. It processes the content line by line with each line representing a tuple or a record of one of the 2 tables. Each field in the tuple is separated by ", ". First field is name of the table and second is the join column value. This join column value is used as key and the entire line is used as value.

## Reducer
Reducer takes the key (join column value) and a list of values (tuples) as input. Reducer separates tuples related to table1 and table2 and place them in 2 separate lists. All the combinations of elements in these two list are generated and written to the output. These combinations make up the rows in the equijoin.
