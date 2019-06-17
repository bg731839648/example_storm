package storm.common.util;

import org.apache.commons.lang.StringUtils;
import storm.common.constant.DateConstant;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtils {

    public static final String GREATER_THAN = ">";
    public static final String LESS_THAN = "<";
    public static final String EQUAL_TO = "==";
    public static final String BE_EQUAL_OR_GREATER_THAN_TO = ">=";
    public static final String BE_EQUAL_OR_LESS_THAN_TO = "<=";

    public static boolean compareStrDate(String date1, String date2, String pattern, String operationalCharacter) {

        boolean result = false;

        try {

            if (StringUtils.isNotBlank(date1) && StringUtils.isNotBlank(date2)) {

                SimpleDateFormat sdf = new SimpleDateFormat(pattern);

                Service date1S = new Service(date1);
                Service date2S = new Service(date2);

                if (GREATER_THAN.equals(operationalCharacter)) {
                    if (sdf.parse(date1S.getTime()).compareTo(sdf.parse(date2S.getTime())) > 0) {
                        result = true;
                    }
                }

                if (LESS_THAN.equals(operationalCharacter)) {
                    if (sdf.parse(date1S.getTime()).compareTo(sdf.parse(date2S.getTime())) < 0) {
                        result = true;
                    }
                }

                if (EQUAL_TO.equals(operationalCharacter)) {
                    if (sdf.parse(date1S.getTime()).compareTo(sdf.parse(date2S.getTime())) == 0) {
                        result = true;
                    }
                }

                if (BE_EQUAL_OR_GREATER_THAN_TO.equals(operationalCharacter)) {
                    if (sdf.parse(date1S.getTime()).compareTo(sdf.parse(date2S.getTime())) >= 0) {
                        result = true;
                    }
                }

                if (BE_EQUAL_OR_LESS_THAN_TO.equals(operationalCharacter)) {
                    if (sdf.parse(date1S.getTime()).compareTo(sdf.parse(date2S.getTime())) <= 0) {
                        result = true;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    static class Service {
        private String time;


        public Service(String time) {
            super();
            this.time = time;
        }

        public String getTime() {
            return time;
        }

        public void setTime(String time) {
            this.time = time;
        }
    }

    public static String formatDate(Date date, String pattern) {
        if (date == null) {
            throw new IllegalArgumentException("date is null");
        } else if (pattern == null) {
            throw new IllegalArgumentException("pattern is null");
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat(DateConstant.YMDHMSS);
            return sdf.format(date);
        }
    }

}
