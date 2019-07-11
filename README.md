# DS_Project5_MapReduce_Spark

These are my codes for project 5 of Distributed System for ISM

Part 1: Using Java MapReduce implementation to complete various calcultion tasks.

Part 2: Using Java Apche Spark to complete various calculations tasks.

Sample Linux commands for using these codes on clusters:

$hadoop dfs -ls /user/userID/input/

$hadoop dfs -copyFromLocal /home/userID/input/1902.txt /user/userID/input/1902.txt

$hadoop dfs -cat /user/userID/input/testFile

$jar -cvf temperature.jar -C  temperature_classes/  .

$hadoop dfs -getmerge /user/userID/output aCoolLocalFile

$hadoop job -list

$bin/hadoop job -kill job_201310251241_0754

$hadoop jar /home/userID/temperature.jar edu.cmu.andrew.mm6.MaxTemperature  /user/userID/input/combinedYears.txt /user/userID/output

$javac -classpath  /usr/local/hadoop/hadoop-core-1.2.1.jar:./temperature_classes -d temperature_classes MaxTemperatureMapper.java

$javac -classpath  /usr/local/hadoop/hadoop-core-1.2.1.jar:./temperature_classes -d temperature_classes MaxTemperatureReducer.java

$javac -classpath  /usr/local/hadoop/hadoop-core-1.2.1.jar:./temperature_classes -d temperature_classes MaxTemperature.java

$mkdir coolProjectOutput

$hadoop dfs -getmerge /user/userID/output ~/coolProjectOutput/

$cat ~/coolProjectOutput/output

