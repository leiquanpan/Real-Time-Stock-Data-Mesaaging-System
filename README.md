Install:
wget http://mirror.cogentco.com/pubcd/apache/flink/flink-1.3.2/flink-1.3.2-bin-hadoop27-scala_2.10.tgz
sudo apt install golang-go
y
mkdir go
(edit the  $HOME/.profile, and add the line GOPATH=’/home/ubuntu/go’, then ‘source’ it)
go get github.com/nsqio/nsq/...

Flink Running
1.put quickstart-0.1.jar into flink-1.3.2/ examples/batch/
2. put pubmess.go into /home/hadoop/
3.put your source file into the HDFS, you can create a file named data_flink, and store the source file into /data_flink/
4.config the hadoop path and GOPATH
export HADOOP_CONF_DIR=/etc/hadoop/conf
export GOPATH=/home/ubuntu/go
5.  Run Flink job
cd /home/hadoop/flink-1.3.2/
./bin/flink run -m yarn-cluster -yn 2 -c org.myorg.quickstart.Application examples/batch/quickstart-0.1.jar 

NSQ and PTPD:

(Running in our case)
1.Running Lookupd 
sudo docker run -d --name nsqdc -p 4152:4152 -p 4153:4153 nsqio/nsq /nsqd --broadcast-address=18.221.119.174 --lookupd-tcp-address=18.221.119.174:4160 --http-address=0.0.0.0:4153 --tcp-address=0.0.0.0:4152
2. Running NSQD deamon
sudo docker run -d --name lookupd -p 4160:4160 -p 4161:4161 nsqio/nsq /nsqlookupd
3. Running PTPD
sudo apt install ptpd

sudo ptpd -i eth0 -M -u 172.31.32.60

4.Running subscriber
go run submess.go “rise_stock”
go run submess.go “down_stock”


# Real-Time-Stock-Data-Mesaaging-System
