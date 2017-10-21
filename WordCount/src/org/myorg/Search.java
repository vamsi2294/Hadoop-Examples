/*########################################VKOVURU@UNCC.EDU###################################
 * ####################################Vamsi Krishna Kovuru###############################
 */

import java.io.IOException;
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


public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
	   
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
	  
	  //Retrieving query from the arguments
	  String userQuery = args[2];
	  //Initialization of configiration object 
	  Configuration config = getConf();
	  
	  //Setting value of user input as configuration attribute
	  config.set("userQuery", userQuery);   
	   
      Job job  = Job .getInstance(getConf(), " search ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job, args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,DoubleWritable> {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
   	  	 //storing the value of user input from configuration attribute
         String userQuery = context.getConfiguration().get("userQuery");
         //converting query to lowercase
         String[] queryWords = userQuery.toLowerCase().split(" ");
         
         //Split with respect to "#####" delimiter		 
         String wordName = line.split("#####")[0];
         String term = line.split("#####")[1];
         //Split with respect to tab to obtain file name
         String fileName = term.split("\\t")[0];
         //split with tab to obtain the tfidf
         Double tfidf = Double.parseDouble(term.split("\\t")[1]);
         
         //finding the user query words with the words from the files
         for(String word : queryWords){
        	 if(word.equals(wordName)){
        		 context.write(new Text(fileName), new DoubleWritable(tfidf));
        	 }
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable> tfidf_values,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  double tfidf_score = 0.0;
    	  
    	  //providing the tfidf values of each file
    	  for(DoubleWritable value : tfidf_values){
    		  tfidf_score += value.get();
    	  }
    	  
    	  context.write(word, new DoubleWritable(tfidf_score));
      }
   }
}
