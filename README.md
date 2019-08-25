# SQL_schema_to_HiveQL
The objective of this project is to read all the schema files present on HDFS and generate files which will have the CREATE Database and table commands, along with the LOAD statement

--------------
Pre-requisites
--------------
1. Have all the schema files downloaded into ONE HDFS folder
2. Create a HDFS folder on which the HQL files will be saved at the end of the program
3. Provide appropriate permissions, so that there is no error while saving the files on the folder
4. The program takes 3 arguments as input:
    a. HDFS path where all the input schema files are present
    b. HDFS path where the HQL scripts need to be saved
    c. HDFS path where all the files to load are present


-------
Process
-------
1. Retrieve the file names via wholetextfiles, where the files names will be present on the keys of the PairedRDD
2. Generate the full paths of the input files, and loop thru these files to generate the HiveQL file; the loop contains below steps:
3. Extract the database name and table name from the file been read
4. Filter out only those lines which contain the column names 
5. Replace the datatypes as applicable. In our example, we are converting the SQL datatype VARCHAR to HIVE Datatype String
6. Ensure that only the datatypes are been processed, column names should remain the same
7. Populate the statements in the final Arraylist 
8. Create database
9. Use Database
10.Create table
11.Load data inpath
12.Convert this Arraylist to RDD via parallelize
13.Write this RDD via saveAsTextFile onto the destination path provided as Argument 2
