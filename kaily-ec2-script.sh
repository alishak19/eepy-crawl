#!/bin/bash

# Use $(pwd) directly instead of assigning to PWD
kvsWorkers=5 # number of kvs workers to launch
flameWorkers=5 # number of flame workers to launch

rm *.jar

# Compile all Java files
javac --source-path src -d bin $(find src -name '*.java')

# Compile and create Crawler.jar
javac -d bin --source-path src src/cis5550/jobs/NewCrawler.java
sleep 1
jar cf new-crawler.jar bin/cis5550/jobs/NewCrawler.class
sleep 1

# Compile and create Indexer.jar
javac -d bin --source-path src src/cis5550/jobs/Indexer.java
sleep 1
jar cf indexer.jar bin/cis5550/jobs/Indexer.class
sleep 1

# Compile and create PageRank.jar
javac -d bin --source-path src src/cis5550/jobs/NewPageRank.java
sleep 1
jar cf new-pagerank.jar bin/cis5550/jobs/NewPageRank.class
sleep 1

# Launch KVS Coordinator
echo "cd '$(pwd)'; java -cp bin cis5550.kvs.Coordinator 8000" > kvscoordinator.sh
chmod +x kvscoordinator.sh
# Run KVS coordinator in the background, log output and errors to specific file
nohup ./kvscoordinator.sh > kvscoordinator.log 2>&1 &

sleep 2

# Launch KVS Workers
for i in `seq 1 $kvsWorkers`
do
    dir=worker$i
    if [ ! -d $dir ]
    then
        mkdir $dir
    fi
    echo "cd '$(pwd)'; java -cp bin -Xmx1024m cis5550.kvs.Worker $((8000+$i)) $dir localhost:8000" > kvsworker$i.sh
    chmod +x kvsworker$i.sh
    # Run each KVS worker in the background and log output/errors to specific file
    nohup ./kvsworker$i.sh > kvsworker$i.log 2>&1 &
done

# Launch Flame Coordinator
echo "cd '$(pwd)'; java -cp bin cis5550.flame.Coordinator 9000 localhost:8000" > flamecoordinator.sh
chmod +x flamecoordinator.sh
# Run Flame coordinator in the background, log output and errors to specific file
nohup ./flamecoordinator.sh > flamecoordinator.log 2>&1 &

sleep 2

# Launch Flame Workers
for i in `seq 1 $flameWorkers`
do
    echo "cd '$(pwd)'; java -cp bin cis5550.flame.Worker $((9000+$i)) localhost:9000" > flameworker$i.sh
    chmod +x flameworker$i.sh
    # Run each Flame worker in the background and log output/errors to specific file
    nohup ./flameworker$i.sh > flameworker$i.log 2>&1 &
done
