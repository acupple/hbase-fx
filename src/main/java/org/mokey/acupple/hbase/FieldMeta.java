package org.mokey.acupple.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.mokey.acupple.hbase.annotations.Entity;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by enousei on 3/10/16.
 */
class FieldMeta {
    private Field field;
    private byte[] hfamily;
    private String family;
    private byte[] qualifier;
    private int ttl;
    private HType type;
    private boolean valid = false;
    private boolean increment = false;
    private HType keyType;
    private HType valueType;

    public FieldMeta(Field field){
        this.field = field;
        try{
            field.setAccessible(true);
            Entity entity = field.getAnnotation(Entity.class);
            if(entity == null){
                return;
            }
            this.ttl = entity.ttl();
            this.family = entity.family();
            this.hfamily = Bytes.toBytes(entity.family());
            if(!entity.name().isEmpty()){
                this.qualifier = Bytes.toBytes(entity.name());
            }else {
                this.qualifier = Bytes.toBytes(field.getName());
            }
            this.increment = entity.increment();
            this.type = HType.parse(field.getType());
            if(this.type == HType.MAP){
                ParameterizedType pt = (ParameterizedType)field.getGenericType();
                Type[] types = pt.getActualTypeArguments();
                this.keyType = HType.parse((Class) types[0]);
                this.valueType = HType.parse((Class) types[1]);
            }
            this.valid = true;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public byte[] getHfamily() {
        return hfamily;
    }

    public String getFamily() {
        return family;
    }

    public Field getField() {
        return field;
    }

    public int getTtl() {
        return ttl;
    }

    public HType getType() {
        return type;
    }

    public HType getKeyType() {
        return keyType;
    }

    public HType getValueType() {
        return valueType;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public boolean isIncrement() {
        return increment;
    }

    public boolean isValid(){
        return this.valid && this.hfamily != null && this.qualifier != null;
    }
}
