package org.mokey.acupple.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.mokey.acupple.hbase.annotations.Entity;
import org.mokey.acupple.hbase.annotations.Table;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by enousei on 3/10/16.
 */
class TableMeta {
    private Class<? extends HBase> clazz;
    private TableName htableName;
    private Map<String, FieldMeta> fieldMap = new HashMap<>();
    private Map<String, Integer> families = new HashMap<>();
    private boolean valid = false;

    public TableMeta(Class<? extends HBase> clazz){
        this.clazz = clazz;
        try{
            Table table = this.clazz.getAnnotation(Table.class);
            if(table == null){
                return;
            }
            String tableName;
            if(table.name().isEmpty()){
                tableName = clazz.getSimpleName();
            }else {
                tableName = table.name();
            }

            this.htableName = TableName.valueOf(tableName);

            Field[] fields = getEntityFileds();

            if(fields == null || fields.length == 0){
                return;
            }

            for (Field field: fields){
                FieldMeta fieldMeta = new FieldMeta(field);
                if(fieldMeta.isValid() && fieldMeta.getType() != HType.UNKNOWN){
                    if(!families.containsKey(fieldMeta.getFamily())){
                        families.put(fieldMeta.getFamily(), fieldMeta.getTtl()); //DODO: First work
                    }
                    this.fieldMap.put(field.getName(), new FieldMeta(field));
                }
            }

            this.valid = true;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public TableName getHtableName() {
        return htableName;
    }

    public boolean isValid() {
        return valid && htableName != null && !fieldMap.isEmpty();
    }

    public HBase parse(Result rs){
        HBase instance = null;
        try {
            if(this.isValid()) {
                instance = clazz.newInstance();
                for (FieldMeta field : fieldMap.values()) {
                    if (field.isValid()) {
                        setValue(instance, field, rs);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return instance;
    }

    public Increment getIncrement(HBase base){
        Increment increment = new Increment(base.getRowKey());
        for (FieldMeta field: fieldMap.values()){
            try {
                if(field.isIncrement()){
                    long amount = field.getField().getLong(base);
                    increment.addColumn(field.getHfamily(), field.getQualifier(), amount);
                }
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }

        return increment;
    }

    public Put getPut(HBase base){
        Put put = new Put(base.getRowKey());
        for (FieldMeta field: fieldMap.values()){
            try {
                Object obj = field.getField().get(base);
                if(field.getType() == HType.MAP && obj != null){
                    Map map = (Map)obj;
                    for (Object key: map.keySet()){
                        put.addColumn(field.getHfamily(),
                                toByte(key, field.getKeyType()),
                                toByte(map.get(key),field.getValueType()));
                    }
                }else {
                    byte[] bytes = toByte(field.getField().get(base), field.getType());
                    if (bytes != null)
                        put.addColumn(field.getHfamily(), field.getQualifier(), bytes);
                }
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }

        return put;
    }

    public HTableDescriptor getDescriptor(){
        HTableDescriptor tableDesc = new HTableDescriptor(htableName);
        for (String family: families.keySet()){
            tableDesc.addFamily(new HColumnDescriptor(family)
                    .setTimeToLive(families.get(family)));
        }
        return tableDesc;
    }

    private Field[] getEntityFileds(){
        List<Field> fields = Lists.newArrayList();
        Field[] thisFields = this.clazz.getDeclaredFields();
        for (Field field: thisFields){
            if(field.getAnnotation(Entity.class) != null){
                fields.add(field);
            }
        }
        Field[] suprtFields = this.clazz.getSuperclass().getDeclaredFields();
        for (Field field: suprtFields){
            if(field.getAnnotation(Entity.class) != null){
                fields.add(field);
            }
        }
        Field[] entities = new Field[fields.size()];
        return fields.toArray(entities);
    }

    private void setValue(Object instance, FieldMeta field, Result rs){
        try{
            if(field.getType() == HType.MAP){
                field.getField().set(instance, new HashMap<>());
                Map map = (Map)field.getField().get(instance);
                NavigableMap<byte[], byte[]> tags = rs.getFamilyMap(field.getHfamily());
                for(Map.Entry<byte[], byte[]> entry : tags.entrySet()){
                    if(entry.getKey().length == 0){
                        continue;
                    }
                    if(entry.getValue().length == 0){
                        map.put(getValue(field.getKeyType(), entry.getKey()), null);
                    }else {
                        map.put(getValue(field.getKeyType(), entry.getKey()),
                                getValue(field.getValueType(), entry.getValue()));
                    }
                }
            } else {
                byte[] value = rs.getValue(field.getHfamily(), field.getQualifier());
                if (value == null || value.length == 0) {
                    return;
                }
                field.getField().set(instance, getValue(field.getType(), value));
            }
        }catch (Throwable ex){
            ex.printStackTrace();
        }
    }

    private Object getValue(HType type, byte[] value) throws Exception{
        switch (type) {
            case INT:
                return Bytes.toInt(value);
            case SHORT:
                return Bytes.toShort(value);
            case CHAR:
                return toChar(value);
            case LONG:
                return Bytes.toLong(value);
            case DOUBLE:
                return Bytes.toDouble(value);
            case FLOAT:
                return Bytes.toFloat(value);
            case STRING:
                return Bytes.toString(value);
            case BINARY:
                return value;
            default:
                return null;
        }
    }

    private byte[] toByte(Object obj, HType type){
        if(obj == null){
            return null;
        }
        switch (type){
            case INT:
                return Bytes.toBytes((int)obj);
            case SHORT:
                return Bytes.toBytes((short)obj);
            case CHAR:
                return Bytes.toBytes((char)obj);
            case LONG:
                return Bytes.toBytes((long)obj);
            case DOUBLE:
                return Bytes.toBytes((double)obj);
            case FLOAT:
                return Bytes.toBytes((float)obj);
            case STRING:
                return Bytes.toBytes((String)obj);
            case BINARY:
                return (byte[])obj;
        }

        return null;
    }

    private char toChar(byte[] value) {
        return (char) ((0xff & value[0]) | (0xff00 & (value[1] << 8)));
    }
}
