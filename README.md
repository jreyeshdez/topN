### Top N

Retrieve a relatively small number of top N records, according to a ranking scheme in your data set. 

### Set Up:
  - Create a folder where to place your input and output:
  ```sh
    $ hdfs dfs -mkdir -p /user/hduser/topn
    $ hdfs dfs -rm -r -f /user/hduser/topn_out
  ```
  - Copy input file to HDFS filesystem:
  ```sh
    $ hdfs dfs -copyFromLocal /home/hduser/topN/input.txt /user/hduser/topn
  ```
  - Compile Groovy code:
    1. Make sure to have set up the environment variable HADOOP_CLASSPATH.
    2. Also, we need to add groovy-all-$VER.jar to HADOOP_CLASSPATH.
    ```sh
    $ groovyc -classpath $HADOOP_CLASSPATH -d classes TopN.groovy
    ```
  - Create JAR file:
    ```sh
    $ jar cvf TopN.jar -C classes .
    ```
  - Run Hadoop job:
    ```sh
    $ hadoop jar TopN.jar TopN /user/hduser/topn /user/hduser/topn_out N
    ```
  - Get results:
    ```sh
    $ hdfs dfs -getmerge /user/hduser/topn_out /home/hduser/topN/output.txt
    ```
    
  
