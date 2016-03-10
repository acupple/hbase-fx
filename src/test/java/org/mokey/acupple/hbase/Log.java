package org.mokey.acupple.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.mokey.acupple.hbase.annotations.Entity;
import org.mokey.acupple.hbase.annotations.Table;
import org.mokey.acupple.hbase.utils.CamUtil;

import java.util.Map;

/**
 * Created by enousei on 3/9/16.
 */
@Table
public class Log implements HBase {
    @Entity(family = "content")
    private int appId;

    @Entity(family = "content")
    private long logId;

    @Entity(family = "content")
    private String env;

    @Entity(family = "content")
    private String envGroup;

    @Entity(family = "content")
    private long logtime;

    @Entity(family = "content")
    private String message;

    @Entity(family = "tags")
    private Map<String, String> tags;

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public long getLogId() {
        return logId;
    }

    public long getLogtime() {
        return logtime;
    }

    public void setLogtime(long logtime) {
        this.logtime = logtime;
    }

    public void setLogId(long logId) {
        this.logId = logId;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getEnvGroup() {
        return envGroup;
    }

    public void setEnvGroup(String envGroup) {
        this.envGroup = envGroup;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public byte[] getRowKey() {
        byte[] appIdHash = Bytes.toBytes(CamUtil.getHashCode(String
                .valueOf(appId))); //4b
        byte[] appIdBytes = Bytes.toBytes(appId); //4b
        byte[] dayBytes = Bytes.toBytes(CamUtil.getRelativeDay(logtime));//1b
        byte[] timeBytes = Bytes.toBytes(CamUtil.getRelativeMillSeconds(logtime));//8b
        byte[] logIdBytes = Bytes.toBytes(logId);//8b

        return CamUtil.concat(appIdHash, appIdBytes, dayBytes,
                timeBytes, logIdBytes);
    }

    public static void main(String[] args){
        HFxClient hFxClient = new HFxClient("127.0.0.1:2181", "/hbase");
        hFxClient.createTable(Log.class);
        Log log = new Log();
        log.setLogId(1);
        log.setEnvGroup("DEV");
        log.setEnv("DEV");
        log.setAppId(9000);
        log.setMessage("this is a test message too");
        log.setLogtime(System.currentTimeMillis());

        hFxClient.insert(log);

        Log log1 = hFxClient.get(log.getRowKey(), Log.class);

        System.out.println(log1.getMessage());
    }
}
