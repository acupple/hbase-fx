package org.mokey.acupple.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by enousei on 3/9/16.
 */
public class HFxClient {
    private final String zk_addr;
    private final String zk_root;

    private Connection connection = null;
    private Map<Class<?>, TableMeta> tableMetaMap = new HashMap<>();

    public HFxClient(String zk_addr, String zk_root){
        this.zk_addr = zk_addr;
        this.zk_root = zk_root;

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", zk_addr);
        conf.set("zookeeper.znode.parent", zk_root);
        try {
            this.connection = ConnectionFactory.createConnection(conf);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    close();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void createTable(Class<? extends HBase> clazz){
        TableMeta tableMeta = getTableMeta(clazz);
        try {
            Admin admin = connection.getAdmin();
            if(admin.tableExists(tableMeta.getHtableName())){
                System.out.println("Table " + tableMeta.getHtableName().getNameAsString() + " is already existed");
                return;
            }

            admin.createTable(tableMeta.getDescriptor());
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public void insert(HBase base){
        TableMeta tableMeta = getTableMeta(base.getClass());
        Put put = tableMeta.getPut(base);
        try {
            Table table = connection.getTable(tableMeta.getHtableName());
            if(put != null)
                table.put(put);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public void insert(List<? extends  HBase> list){
        if(list != null && !list.isEmpty()){
            TableMeta tableMeta = getTableMeta(list.get(0).getClass());
            List<Put> puts = Lists.newArrayList();
            for (HBase hBase: list){
                Put put = tableMeta.getPut(hBase);
                if(put != null) {
                    puts.add(put);
                }
            }
            try {
                Table table = connection.getTable(tableMeta.getHtableName());
                table.put(puts);
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }

    public <T extends HBase> T get(byte[] rowkey, Class<? extends HBase> clazz){
        if(rowkey == null)
            return null;
        TableMeta tableMeta = getTableMeta(clazz);
        Get get = new Get(rowkey);
        try {
            Table table = connection.getTable(tableMeta.getHtableName());
            Result result = table.get(get);
            return (T)tableMeta.parse(result);
        }catch (Exception ex){
            ex.printStackTrace();
        }

        return null;
    }

    public <T extends HBase>  List<T> search(List<byte[]> rowkeys, FilterBase filter, Class<? extends HBase> clazz){
        TableMeta tableMeta = getTableMeta(clazz);

        List<Get> gets = Lists.newArrayList();
        for (byte[] rowkey : rowkeys) {
            Get get = new Get(rowkey);
            if (null != filter) {
                get.setFilter(filter);
            }
            gets.add(get);
        }

        List<T> hBaseList = Lists.newArrayList();
        try {
            Table table = connection.getTable(tableMeta.getHtableName());
            Result[] results = table.get(gets);
            for (Result rs: results){
                HBase hBase = tableMeta.parse(rs);
                if(hBase != null){
                    hBaseList.add((T)hBase);
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }

        return hBaseList;
    }

    public <T extends HBase> List<T> scan(byte[] startRow, byte[] stopRow, Class<? extends HBase> clazz){
        TableMeta tableMeta = getTableMeta(clazz);
        List<T> list = Lists.newArrayList();
        try {
            Table table = connection.getTable(tableMeta.getHtableName());
            ResultScanner results = table.getScanner(new Scan(startRow, stopRow));
            for (Result rs: results){
                T hBase = (T)tableMeta.parse(rs);
                if(hBase != null){
                    list.add(hBase);
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }

        return list;
    }

    public void deleteTable(Class<HBase> clazz){
        TableMeta tableMeta = getTableMeta(clazz);
        try {
            Admin admin = connection.getAdmin();
            if(admin.tableExists(tableMeta.getHtableName())){
                admin.disableTable(tableMeta.getHtableName());
                admin.deleteTable(tableMeta.getHtableName());
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    private TableMeta getTableMeta(Class<? extends HBase> clazz){
        TableMeta tableMeta = tableMetaMap.get(clazz);
        if(tableMeta == null){
            tableMeta = new TableMeta(clazz);
            if(tableMeta.isValid()){
                tableMetaMap.put(clazz, tableMeta);
            }
        }
        return tableMeta;
    }
}
