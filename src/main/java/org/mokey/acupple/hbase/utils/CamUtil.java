package org.mokey.acupple.hbase.utils;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Created by enousei on 3/10/16.
 */
public class CamUtil {
    public static int getHashCode(String str) {
        int hash, i;
        char[] arr = str.toCharArray();
        for (hash = i = 0; i < arr.length; ++i) {
            hash += arr[i];
            hash += (hash << 12);
            hash ^= (hash >> 4);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return hash;
    }

    public static byte[] concat(byte[]... arrays) {
        int length = 0;
        for (byte[] array : arrays) {
            length += array.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }
        return result;
    }

    public static Integer getRelativeDay(long time) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTimeInMillis(time);
        return 365 - calendar.get(Calendar.DAY_OF_YEAR);
    }

    public static Integer getRelativeMillSeconds(long time) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTimeInMillis(time);
        return Integer.valueOf(3600 * 24 * 1000 - (calendar
                .get(Calendar.HOUR_OF_DAY)
                * 3600
                * 1000
                + calendar.get(Calendar.MINUTE)
                * 60
                * 1000
                + calendar.get(Calendar.SECOND) * 1000 + calendar
                .get(Calendar.MILLISECOND)));
    }
}
