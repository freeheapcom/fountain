# Distributed Crawler Project


To run this, you need 2 choices:

1. Local interaction with a local Cassandra server by first starting a local C* process 
   and run the job from the IDE or use run_cmd.sh

2. How to build:
   To generate idea project file: 
       gradlew idea

   To compile:
       gradlew clean build

3. Deploying into the cloud environment the jar file and use run_cmd.sh to execute the spark job

How to build a jar

```
  ./gradlew check
```

To produce a shadowed jar suitable for `spark-submit`,

```
  ./gradlew shadowJar
```


Upload jar to a Spark instance and use scripts/run_cmd.sh to execute the job

