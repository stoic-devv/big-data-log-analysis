# Project build and run configuration

> :warning: if you already have the hortonbox setup and just want to jar run instructions [skip here](#run-jar-file-with-arguments)
### Compile project into a fat jar
```console
# change to project dir
foo@bar:~$ cd <project-dir-path>
# create a fat jar
foo@bar:<project-dir-path>$ sbt assembly
```

### Creating user and hadoop filesystem directories
```console
# log in to hortonbox - ip address is displayed on the vm console
foo@bar:~$ ssh -p 2222 root@<hortonbox-ip-addr>
# add user hw-user
root@sandbox-hdp~$ sudo useradd hw-user
root@sandbox-hdp~$ sudo su hdfs
# make a hadoop filesystem directory
hdfs@sandbox-hdp~$ hadoop fs -mkdir /user/hw-user
# give crud permissions to our user
hdfs@sandbox-hdp~$ hadoop fs -chown hw-user /user/hw-user
# change to our user
hdfs@sandbox-hdp~$ sudo su hw-user
# create hadoop filesystem output directory
# if this command doesn't work it means that user and permissions to the hdfs have not been given properly
hw-user@sandbox-hdp~$ hadoop fs -mkdir /user/hw-user/input /user/hw-user/output
```
### Create directory to store jar file and data
```console
# create jar and data holding directory
root@sandbox-hdp~$ mkdir hw-dir
# create a user group
root@sandbox-hdp~$ sudo groupadd hw-usr-grp
# add root to this user group
root@sandbox-hdp~$ sudo usermod -a -G hw-usr-grp root
# add our hw-user to this user group
root@sandbox-hdp~$ sudo usermod -a -G hw-usr-grp hw-user
# add edit access to the created directory
root@sandbox-hdp~$ chown -R hw-usr-grp hw-dir
# validate if hw-user is able to access this dir
root@sandbox-hdp~$ sudo su hw-user
hw-user@sandbox-hdp~$ ls /root/hw-dir
```
### Copy our jar file and data to hortonbox
```console
# copy jar file. Default jar location: <project-dir-path>/target/scala-3.0.2/
foo@bar:<project-dir-path>$ scp -P 2222 <path-to-jar> root@<hortonbox-ip-addr>:/hw-dir
# copy log file to hw-dir
# sample jars attached in <project-dir>/src/main/resources
foo@bar:<project-dir-path>$ scp -P 2222 <path-to-log-file> root@<hortonbox-ip-addr>:/hw-dir
# log in 
```
### Run jar file with arguments
```console
# run the hadoop jar 
hdfs@sandbox-hdp~$ hadoop jar /hw-dir/<jar directory> <arg-one> <arg-two> /user/hw-user/input

```

## Troubleshooting

1. Connection refused error: This usually happens when hadoop is run in safe mode. To leave the safe mode run:<br />
```hdfs@sandbox-hdp~$ sudo -u hdfs hdfs dfsadmin -safemode leave```
2. Output file already exists: The job you specified has been already run once before. To delete the output drectory: <br />
```hw-user@sandbox-hdp~$ hadoop fs -rm -r -f /user/hw-user/output/<output-dir>```