package org.mokey.acupple.hbase;

/**
 * Created by enousei on 3/10/16.
 */
public enum HType {
    CHAR,
    INT,
    SHORT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    UNKNOWN;

    public static HType parse(Class<?> clazz){
        HType type = HType.UNKNOWN;
        if(clazz.equals(int.class) || clazz.equals(Integer.class)){
            type = INT;
        } else if(clazz.equals(short.class) || clazz.equals(Short.class)){
            type = SHORT;
        }else if(clazz.equals(char.class) || clazz.equals(Character.class)){
            type = CHAR;
        }else if(clazz.equals(long.class) || clazz.equals(Long.class)){
            type = LONG;
        }else if(clazz.equals(float.class) || clazz.equals(Float.class)){
            type = FLOAT;
        }else if(clazz.equals(double.class) || clazz.equals(Double.class)){
            type = DOUBLE;
        }else if(clazz.equals(String.class)){
            type = STRING;
        }
        return type;
    }
}
