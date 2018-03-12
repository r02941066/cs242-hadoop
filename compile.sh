# Update HADOOP CLASSPATH variable
export HADOOP_CLASSPATH=$(hadoop classpath)
#echo $HADOOP_CLASSPATH

# Now compile and create a jar
javac -classpath $HADOOP_CLASSPATH WordCount.java
jar cf wc.jar WordCount*.class

# Delete output directory
hdfs dfs -rm -r /user/tchou006/wordcount/output

# run WordCount
hadoop jar wc.jar WordCount /user/tchou006/wordcount/input /user/tchou006/wordcount/output
