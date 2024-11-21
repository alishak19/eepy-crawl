#!/bin/bash

PWD=`pwd`
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
javac -d bin --source-path src src/cis5550/jobs/PageRank.java
sleep 1
jar cf pagerank.jar bin/cis5550/jobs/PageRank.class
sleep 1

# Start tmux session
tmux new-session -d -s crawler-session

# Launch KVS Coordinator in window 1
tmux new-window -t crawler-session:1 -n "KVS Coordinator" "cd '$PWD'; java -cp bin cis5550.kvs.Coordinator 8000"

# Launch KVS Workers in separate windows (windows 2, 3, 4, ...)
for i in `seq 1 $kvsWorkers`
do
    tmux new-window -t crawler-session:$((i+1)) -n "KVS Worker $i" "cd '$PWD'; java -cp bin cis5550.kvs.Worker $((8000+i)) worker$i localhost:8000"
done

# Launch Flame Coordinator in window after KVS Workers
tmux new-window -t crawler-session:$((kvsWorkers+2)) -n "Flame Coordinator" "cd '$PWD'; java -cp bin cis5550.flame.Coordinator 9000 localhost:8000"

# Launch Flame Workers in separate windows after Flame Coordinator
for i in `seq 1 $flameWorkers`
do
    tmux new-window -t crawler-session:$((kvsWorkers+2+i)) -n "Flame Worker $i" "cd '$PWD'; java -cp bin cis5550.flame.Worker $((9000+i)) localhost:9000"
done

# Attach to the tmux session
tmux attach-session -t crawler-session