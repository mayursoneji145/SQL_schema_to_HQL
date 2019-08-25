package spark.schemaread;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class scread {
	
	public static void main(String[] args) {
		
// The program expects 3 input parameters
//		1. HDFS path where all the Schema files are present
//		2. HDFS path where you want to save the HQL files to 
//		3. HDFS path where the data files are present for loading into HIVE 
		
		
//Setting the log level to only Error

		Logger.getLogger("org").setLevel(Level.ERROR);


//Initializing the Spark Configuration and setting up Spark Context.

		SparkConf conf = new SparkConf().setAppName("Schema_Read");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);

		
//Determining the file names in the input folder; all the keys will be the file names		
		JavaPairRDD<String , String> files = sc.wholeTextFiles(args[0]);
		
		
		JavaRDD<String> fls1 = files.keys().map(
				x-> {	
					String[] vals = x.split("/");
					return vals[vals.length -1];
				}
		);

		
		
//Creating an RDD for naming the files as HQL while generating outputs		
		JavaRDD<String> hqlfnamerdd = fls1.map(
				x -> {
						return x.replaceAll("txt", "hql");
					 }
				);
		
		
// Variables declarations before processing
		
		ArrayList<String> filenames = new ArrayList<String>();
		ArrayList<String> filepaths = new ArrayList<String>();
		ArrayList<String> hqlfpath  = new ArrayList<String>();
		ArrayList<String> databases = new ArrayList<String>();
		ArrayList<String> tables    = new ArrayList<String>();
		ArrayList<String> hqlwrite  = new ArrayList<String>();
		
		String inpfile, opfile, createdb, usedb, createtb, loaddata;
			   inpfile= opfile= createdb= usedb= createtb= loaddata="";
		String endcreatetb=");";
		
		
		
// Determining the full path and getting the filepaths arraylist ready 		
		for (String flss : fls1.collect()) {
			filenames.add(flss);
			inpfile = (args[0] + flss);
			filepaths.add(inpfile);
		}
		
		System.out.println("");
		System.out.println("----------------------------------------------------------------");
		System.out.println("HQL files will be created as mentioned on the paths given below");
		System.out.println("----------------------------------------------------------------");
		System.out.println("");
		
		for (String hfnamedisp : hqlfnamerdd.collect()) {
			opfile = (args[1] + hfnamedisp);
			hqlfpath.add(opfile);
			System.out.println(opfile);
		}
		
		

// Looping thru all the files present on the input folder specified
		
		for(int i=0; i< filepaths.size();i++) {
			JavaRDD<String> fls2  = sc.textFile(filepaths.get(i));

			
// Determining the Database name and Table name from each of the files
			
			for(String datab : fls2.collect()) {
				if (datab.contains("Database")){
					String[] datarr = datab.split(":");
					databases.add(datarr[2]);
				}
				if (datab.contains("CREATE TABLE")){
					String[] tabarr = datab.split("\\s+");
					tables.add(tabarr[2]);
				}
			}
		
		System.out.println("");
		System.out.println("");
		System.out.println("Processing file with Database: " + databases.get(i) + "	Table : " + tables.get(i) +"........");
		System.out.println("");
		System.out.println("");
			
// Determining the column names and the datatypes
			
			JavaRDD<String> fls3 = fls2.map(

					x -> {
							String retval = "";
							String[] vals = x.split(";");
							for(int k=0; k<vals.length;k++) {
								if (!vals[k].contains("--") && !vals[k].contains("/*") && vals[k].contains(",")) {
									retval = x;
								}
								else 
									retval = "";
							}
							return retval;
					}
			);
			
			
			JavaRDD<String> fls4 = fls3.filter(row-> row!="");

// Changing the type Varchar in SQL to String in HIVE
			
			JavaRDD<String> remnum = fls4.map(
					
					x -> {
							String[] interim = x.split("\\s+");
							String intm =  interim[2].replaceAll("[0-9|(|)]", "");
							String intm1 = intm.replaceAll("varchar", "string");
							return(interim[1] + " " + intm1 + ",");
						 }
					);
					
	
// Populating the Hive QL File
			
			createdb = ("create database if not exists " + databases.get(i) + " ;");
			usedb =    ("use " + databases.get(i) + " ;");
			createtb = ("create table if not exists " + tables.get(i) + " (");		
			loaddata = ("load data inpath '" + args[2] + filenames.get(i) + "' into table " + tables.get(i) + ";");
	
			hqlwrite.clear();
			hqlwrite.add(createdb);
			hqlwrite.add(usedb);
			hqlwrite.add(createtb);
			
			for (String remnumprocess : remnum.collect()) {
				hqlwrite.add(remnumprocess);
			}
			
			hqlwrite.add(endcreatetb);
			hqlwrite.add(loaddata);
			
// Removing the Comma (,) on the last column on the Create table query			
			hqlwrite.set(hqlwrite.size()-3, hqlwrite.get(hqlwrite.size()-3).replaceAll(",",""));
			
/*			for(int o=0;o<hqlwrite.size();o++) {
				System.out.println(hqlwrite.get(o));
			}
*/			
			
// Converting the Arraylist to RDD			
			JavaRDD<String> hqlrdd=sc.parallelize(hqlwrite);
			
// Saving the RDD in the file path provided by the user (args[1])			
			hqlrdd.saveAsTextFile(hqlfpath.get(i));

			System.out.println("------------------------------------------------------------");
			System.out.println("End of File Processing " + (i+1));
			System.out.println("------------------------------------------------------------");
			
		}
		
	sc.stop();
	}
}
