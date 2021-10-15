
## Local build
```console
# create a fat jar
$ sbt assembly
# copy the jar to the root
$ scp -P 2222 target/scala-3.0.2/big-data-log-analysis-assembly-0.1.jar root@192.168.222.128:~/hw-dir
$ ssh -p 2222 root@192.168.222.128 # log in to hortonbox
```

## Hortonbox

First steps:
These steps are one-time only. For subsequent executions skip these.

```console
# add user hw-user
foo@bar:~$ sudo useradd hw-user
$ sudo su hdfs
# make a hadoop filesystem directory
$ hadoop fs -mkdir /user/hw-user
# give crud permissions to our user
$ hadoop fs -chown hw-user /user/hw-user
# change to our user
$ sudo su hw-user
# create hadoop filesystem output directory
$ hadoop fs -mkdir /user/hw-user/output

```
## Troubleshoot
1. Output directory already exists : delete the output directory
```console

```
2. 

