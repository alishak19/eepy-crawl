#!/bin/bash

PWD=`pwd`
kvsWorkers=1 # number of kvs workers to launch
flameWorkers=1 # number of flame workers to launch

rm -r worker1
rm *.jar

# Compile all Java files
javac --source-path src -d bin $(find src -name '*.java')

# Launch KVS Coordinator
echo "cd '$(PWD)'; java -cp bin cis5550.kvs.Coordinator 8000" > kvscoordinator.sh
chmod +x kvscoordinator.sh
open -a Terminal kvscoordinator.sh

sleep 2

# Launch KVS Workers
for i in `seq 1 $kvsWorkers`
do
    dir=worker$i
    if [ ! -d $dir ]
    then
        mkdir $dir
    fi
    echo "cd '$(PWD)'; java -cp bin cis5550.kvs.Worker $((8000+$i)) $dir localhost:8000" > kvsworker$i.sh
    chmod +x kvsworker$i.sh
    open -a Terminal kvsworker$i.sh
done

# Launch Flame Coordinator
echo "cd '$(PWD)'; java -cp bin cis5550.flame.Coordinator 9000 localhost:8000" > flamecoordinator.sh
chmod +x flamecoordinator.sh
open -a Terminal flamecoordinator.sh

sleep 2

# Launch Flame Workers
for i in `seq 1 $flameWorkers`
do
    echo "cd '$(PWD)'; java -cp bin cis5550.flame.Worker $((9000+$i)) localhost:9000" > flameworker$i.sh
    chmod +x flameworker$i.sh
    open -a Terminal flameworker$i.sh
done
