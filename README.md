# hbase-fx
Simple Hbase ORM

# usage:
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
