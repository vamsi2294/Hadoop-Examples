# Hadoop-Examples
Cloud Computing 
Instructions:

-MapReduce jobs were executed in Cloudera. 
All the canterbury files are moved from local file system to the input folder in Hadoop file system.
Command: Hadoop fs -put /<local file path>/ /<input FilePath>/

DocWordCount: 
-To run it must be provided with two arguments:input path and output path.
Command: Hadoop jar <Jar name> DocWordCount /<file path>/input /<filepath>/DocWordCountOutput

TermFrequency:
-To run it must be provided with two arguments:input path and output path.
Command: Hadoop jar <Jar name> TermFrequency /user/<filepath>/input /user/<filepath>/TermFrequencyOutputTFIDF:
-To run TFIDF job it must be provided with two arguments:input path and output path.
Command: Hadoop jar <Jar name> TFIDF /user/<username>/input /user/<username>/TFIDFOutput
(if you want to rerun TFIDF delete the IntermediateOutput file from the path with)
Remove output file Command:Hadoop -rm -r /<filepath>/IntermediateOutput)Search:
-We need to run TFIDF job to perform Search job, The output from tfidf and output path must be provided and search query
Command: Hadoop jar <Jar name> Search /<Filepath>/TFIDFOutput /<Filepath>/SearchOutput â€œQUERYâ€

	
