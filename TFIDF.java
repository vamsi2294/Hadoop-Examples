/*########################################VKOVURU@UNCC.EDU###################################
 * ####################################Vamsi Krishna Kovuru###############################
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
	   
	   String[] intermediate_args = {args[0],"TempOutput"};
	  //Running TermFrequency job
	  int termFrequencyRes  = ToolRunner .run( new TermFrequency(), intermediate_args);
	  
	  //Running TFIDF job
      int tfidfRes  = ToolRunner .run( new TFIDF(), args);
      
      System .exit(termFrequencyRes);
      System .exit(tfidfRes);
   }

   public int run( String[] args) throws  Exception {
	   
	  //Initialization of configiration object 
	  Configuration config = getConf();
	  FileSystem fileSystem = FileSystem.get(config);
	  
	  //Configuring Total files
	  final int totalFiles = fileSystem.listStatus(new Path(args[0])).length;
	  config.setInt("totalFiles", totalFiles);   
	   
      Job job  = Job .getInstance(getConf(), " tfidf ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job, "TempOutput");
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,Text > {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
         //Initialization of Key value pairs to Text object
         Text key=new Text();
         Text value=new Text();
         
         
         //Split with respect to "#####" delimiter
         String wordName = line.split("#####")[0];
         String term = line.split("#####")[1];
         String fileName = term.split("\\t")[0];
         
         //storing value as filename=term frequency
         String value_term = fileName + "=" + term.split("\\t")[1];
         
         key  = new Text(wordName);
         value  = new Text(value_term);
         context.write(key,value);
      }
   }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text> valueWords,  Context context)
         throws IOException,  InterruptedException {
    	  //storing the value of total files from configuration attribute
    	  int totalFiles = context.getConfiguration().getInt("totalFiles", 0);	  
    	
    	  double tf_idf,idf;
    	  int reFiles = 0;
    	  List<String> fileName = new ArrayList<String>();
    	  List<Double> fequency = new ArrayList<Double>();
    	  //loop to store file names and word names in the array lists
    	  for(Text value : valueWords){
    		  String[] x = value.toString().split("=");
    		  fileName.add(x[0]);
    		  fequency.add(Double.parseDouble(x[1]));
    		  reFiles = reFiles + 1; 
    	  }
    	  //Calculation of IDF
    	  idf = Math.log10(1+(totalFiles/fileName.size()));
    	  
    	  //Calculation of TFIDF for each word
    	  for(int i = 0;i<fileName.size();i++){
    		  tf_idf = idf * fequency.get(i);
    		  context.write(new Text(word.toString()
    				  .concat("#####")
    				  .concat(fileName.get(i))),new DoubleWritable(tf_idf));
    	  }
    	     	  
      }
   }
}
