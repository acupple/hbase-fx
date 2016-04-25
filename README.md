# hbase-fx
HBase-fx is a simple hbase ORM framework, which can be used to:

1. create table if not exists
2. delete table
3. IINSERT, INCREMENT, GET, SEARCH, SCAN

# usage:
```bash
1. clone the project
git clone git@github.com:acupple/hbase-fx.git
2. install
cd hbase-fx
mvn clean install -DskipTests
3. import dependency
<dependency>
    <groupId>org.mokey.acupple</groupId>
    <artifactId>hbase-fx</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
# example
```java
public static void main(String[] args){
    HFxClient hFxClient = new HFxClient("127.0.0.1:2181", "/hbase");
    hFxClient.creqteTable(Log.class);
    Log log = new Log();
    log.setLogId(1);
    log.setEnvGroup("DEV");
    log.setEnv("DEV");
    log.setAppId(9000);
    log.setMessage("this is a test message");
    log.setLogtime(System.currentTimeMillis());

    hFxClient.insert(log);

    Log log1 = (Log)hFxClient.get(log.getRowKey(), Log.class);

    System.out.println(log1.getMessage());
}
```
