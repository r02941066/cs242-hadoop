# Update HADOOP CLASSPATH variable
export HADOOP_CLASSPATH=$(hadoop classpath)
#echo $HADOOP_CLASSPATH

# Now compile and create a jar
javac -classpath $HADOOP_CLASSPATH WordCount.java
jar cf wc.jar WordCount*.class

inputDir=/user/tchou006/wordcount/input
remoteDir=/user/tchou006/wordcount/output

if [ $# == 0 ]; then
	# Delete output directory
	hdfs dfs -rm -r $remoteDir
elif [ $# == 1 ]; then
	remoteDir=$1
elif [ $# == 2 ]; then
	inputDir=$1
	remoteDir=$2
else
	echo "As 0 input"
	hdfs dfs -rm -r $remoteDir
fi

# run WordCount
hadoop jar wc.jar WordCount $inputDir $remoteDir
