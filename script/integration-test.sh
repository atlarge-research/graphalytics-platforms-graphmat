set -e

#cd /home/wlngai/Workstation/Repo/tudelft-atlarge/granula/granula-modeller/graphmat/1.0/analyzer
#mvn clean install -DskipTests -q

#cd /home/wlngai/Workstation/Repo/tudelft-atlarge/granula/granula-archiver
#mvn clean install -DskipTests -q

#cd /home/wlngai/Workstation/Repo/tudelft-atlarge/graphalytics
#mvn clean install -Pgranula -DskipTests -q

cd /home/wlngai/Workstation/Repo/tudelft-atlarge/graphalytics-platforms-graphmat
mvn clean package  -DskipTests -q

cd /home/wlngai/Workstation/Data/graphmat/dist
rm -Rf graphalytics*

cp /home/wlngai/Workstation/Repo/tudelft-atlarge/graphalytics-platforms-graphmat/graphalytics-0.3-SNAPSHOT-graphmat-0.1-SNAPSHOT-bin.tar.gz .
tar -zxvf graphalytics-0.3-SNAPSHOT-graphmat-0.1-SNAPSHOT-bin.tar.gz
cp -r config graphalytics-0.3-SNAPSHOT-graphmat-0.1-SNAPSHOT

cd /home/wlngai/Workstation/Data/graphmat/dist/graphalytics-0.3-SNAPSHOT-graphmat-0.1-SNAPSHOT
filename=run$(date -d "today" +"%m%d-%H%M").log
./run-benchmark.sh &> $filename

cat $filename
