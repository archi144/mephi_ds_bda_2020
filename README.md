#Hadoop Longest words

It is app, which finds the longest word[s] in file[s].

Minimal system requirements
--
Java 8  
Apache Maven 4.0.0  
Hadoop 3.1.1 
###Prepare data

```sh
[yourUser@localhost homework1]# sh generateInputData.sh <Count of generated words>
```

###Put data to hdfs
```sh
[yourUser@localhost homework1]# hdfs dfs -put input/
```
It will copy date from ./input to hdfs user/**yourUser**/input. Apart from it, folders user/**yourUser**/input and /user/**yourUser**/output/ will be erased.

##Compile jar
```sh
[yourUser@localhost homework1]# mvn install
```

##Run programm
```sh
[yourUser@localhost homework1]# hadoop jar /path/to/builded/homework1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
```