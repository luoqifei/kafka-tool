# kafka-tool
   use my kafka tool,you can count the topic message's number by timestamp.you just need to input the
start timestamp and end timestamp(they are unix timestamp).then this tool will printout how much messages
between start-end time area for each partitions.

## dependencies

### JDK 8

### maven 


## package

mvn clean package

## run

java -jar kafka-tools-lqf-jar-with-dependencies.jar

## attach 
1 add kafka security producer and consumer client demo
2 add java shell control remote cluster authorizer and acls demo