package com.largeyes.plat.paas.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;

public class DateUtil {

    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static final String YYYYMMDD = "yyyyMMdd";

    public static final String YYYYMMDDHHMM = "yyyyMMddHHmm";
    
    public static final String HHMMSS = "HHmmss";

    /**
     * 获取当前时间戳
     * 
     * @return
     * @author zhangchao
     */
    public static long getCurrentTimeMillis() {
        Timestamp time = getSysDate();
        return time.getTime();
    }

    /**
     * 获取系统时间[仅在SV层以下使用]
     * 
     * @return
     * @author zhangchao
     */
    public static Timestamp getSysDate() {
        Timestamp time = new Timestamp(Calendar.getInstance().getTimeInMillis());
        return time;
    }

    /**
     * 根据指定的格式输入时间字符串
     * 
     * @param pattern
     * @return
     * @author zhangchao
     */
    public static String getDateString(String pattern) {
        Timestamp time = getSysDate();
        DateFormat dfmt = new SimpleDateFormat(pattern);
        java.util.Date date = time;
        return dfmt.format(date);
    }

    /**
     * 获取时间字符串
     * 
     * @param time
     * @param pattern
     * @return
     * @author zhangchao
     */
    public static String getDateString(Timestamp time, String pattern) {
        DateFormat dfmt = new SimpleDateFormat(pattern);
        java.util.Date date = time;
        return date != null ? dfmt.format(date) : "";
    }

    /**
     * 获取指定时间的格式化串
     * 
     * @param date
     * @param pattern
     * @return
     * @author zhangchao
     */
    public static String getDateString(Date date, String pattern) {
        SimpleDateFormat sdfmt = new SimpleDateFormat(pattern);
        return date != null ? sdfmt.format(date) : "";
    }

    /**
     * 判断时间是否符合格式要求
     * 
     * @param str
     * @param fomat
     * @return
     * @author zhangchao
     */
    public static boolean isValidDate(String str, String fomat) {
        boolean flag = true;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(fomat);
            sdf.parse(str);
            flag = true;
        } catch (ParseException e) {
            flag = false;
        }
        return flag;
    }

    /**
     * 字符串转日期（精确到日）
     * 
     * @param str
     * @return
     * @author zhangchao
     */
    public static Date str2Date(String str) {
        Date date = null;
        if (!StringUtil.isBlank(str)) {
            date = DateUtil.to_date(str, DATE_FORMAT);
        }
        return date;

    }

    public static Timestamp getFutureTime() {
        Date d = str2Timestamp("2100-01-01 00:00:00");
        Timestamp beforeSecond = getBeforeSecond(new Timestamp(d.getTime()));
        return beforeSecond;
    }

    /**
     * 转换为时间
     * 
     * @param str
     * @return
     * @author zhangchao
     */
    public static Date str2Timestamp(String str) {
        Date date = null;
        if (!StringUtil.isBlank(str)) {
            date = DateUtil.to_date(str, DATETIME_FORMAT);
        }
        return date;

    }

    /**
     * 按指定格式将字符串转换为日期对象
     * 
     * @param dateStr
     * @param format
     * @return
     * @author zhangchao
     */
    public static Date to_date(String dateStr, String format) {
        DateFormat df = new SimpleDateFormat(format);
        try {
            java.util.Date date = df.parse(dateStr);
            Date d = new Date(date.getTime());
            return d;
        } catch (Exception e) {
            throw new RuntimeException("系统转换日期字符串时出错！", e);
        }
    }

    /**
     * 获取日期 如：2011-12-12
     * 
     * @return
     * @author zhangchao
     */
    public static Date getDate() {
        String s = getDateString(DATETIME_FORMAT);
        Date a = DateUtil.str2Date(s);
        return a;
    }

    /**
     * 获取某个时间点所代表的日期
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Date getTheDayDate(Timestamp sysDate) {
        DateFormat dfmt = new SimpleDateFormat(DATETIME_FORMAT);
        java.util.Date date = sysDate;
        String dateString = dfmt.format(date);
        Date a = DateUtil.str2Date(dateString);
        return a;
    }

    /**
     * 获取指定时间点偏移天数后的日期
     * 
     * @param sysDate
     * @param offsetDays
     * @return
     * @author zhangchao
     */
    public static Date getOffsetDaysDate(Timestamp sysDate, int offsetDays) {
        Timestamp t = getOffsetDaysTime(sysDate, offsetDays);
        Date d = getTheDayDate(t);
        return d;
    }

    public static Date getOffsetDaysDate(Date date, int offsetDays) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, offsetDays);
        return new Date(calendar.getTimeInMillis());
    }

    /**
     * 获取一天的第一秒 如：2011-11-11 00:00:00
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTheDayFirstSecond(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取一天的最后一秒 如：2011-11-11 23:59:59
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTheDayLastSecond(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, -1);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取指定时间的偏移天数后的时间
     * 
     * @param sysDate
     * @param offsetDays
     * @return
     * @author zhangchao
     */
    public static Timestamp getOffsetDaysTime(Timestamp sysDate, int offsetDays) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.DAY_OF_MONTH, offsetDays);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取指定时间的偏移月份后的时间
     * 
     * @param sysDate
     * @param offsetDays
     * @return
     * @author yangpy
     */
    public static Timestamp getOffsetMonthsTime(Timestamp sysDate,int offsetDays) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.MONTH, offsetDays);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取指定时间的偏移年份后的时间
     * 
     * @param sysDate
     * @param offsetDays
     * @return
     * @author zhangxianwei
     */
    public static Timestamp getOffsetYearsTime(Timestamp sysDate, int offsetDays) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.YEAR, offsetDays);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取本月最后一秒
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTimeThisMonthLastSec(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.MONTH, 1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, -1);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取本月第一秒
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTimeThisMonthFirstSec(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取下月第一秒
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTimeNextMonthFirstSec(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.MONTH, 1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }
    
    /**
     * 获取本年第一秒
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTimeThisYearFirstSec(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }
    
    /**
     * 获取本周第一秒
     * 
     * @param sysDate
     * @return
     * @author zhangchao
     */
    public static Timestamp getTimeThisWeekFirstSec(Timestamp sysDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sysDate);
        calendar.add(Calendar.WEEK_OF_MONTH, -1);
        calendar.set(Calendar.DAY_OF_WEEK, 2);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取当月总的天数
     * 
     * @return
     * @author zhangchao
     */
    public static int getDaysOfThisMonth() {
        Timestamp currTimestamp = DateUtil.getSysDate();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currTimestamp);
        return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    }

    /**
     * 从yyyyMM格式中获取月
     * 
     * @param yyyyMM
     * @return
     * @author zhangchao
     */
    public static String getMonth(String yyyyMM) {
        if (StringUtil.isBlank(yyyyMM) || yyyyMM.length() != 6) {
            throw new RuntimeException("格式出错，无法获取月");
        }
        String month = yyyyMM.substring(4, 6);
        return month;
    }

    /**
     * 验证日期格式 yyyyMM、yyyyMMdd、yyyyMMddHHmmss
     * 
     * @param str
     * @return
     * @author shanxf
     */
    public static boolean isDateType(String str) {
        String withYYYYMMDDHHSSRegax = "^\\d{4}([1-9]|(1[0-2])|(0[1-9]))([1-9]|([12]\\d)|(3[01])|(0[1-9]))(([0-1][0-9])|([2][0-3]))([0-5][0-9])([0-5][0-9])$";
        String withYYYYMMDDRegax = "^\\d{4}([1-9]|(1[0-2])|(0[1-9]))([1-9]|([12]\\d)|(3[01])|(0[1-9]))$";
        String withYYYYMMRegax = "^\\d{4}((1[0-2])|(0[1-9]))$";
        if (StringUtils.isEmpty(str))
            return false;
        if (str.length() == 6)
            return str.matches(withYYYYMMRegax);

        if (str.length() == 8)
            return str.matches(withYYYYMMDDRegax);

        if (str.length() == 14)
            return str.matches(withYYYYMMDDHHSSRegax);

        return false;
    }

    /**
     * 计算两个日期的时间差(天)
     * 
     * @param formatTime1
     * @param formatTime2
     * @return
     */
    public static long getTimeDifference(Timestamp formatTime1,Timestamp formatTime2) {
        SimpleDateFormat timeformat = new SimpleDateFormat(
                "yyyy-MM-dd,HH:mm:ss");
        long t1 = 0L;
        long t2 = 0L;
        try {
            t1 = timeformat.parse(getTimeStampNumberFormat(formatTime1))
                    .getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        try {
            t2 = timeformat.parse(getTimeStampNumberFormat(formatTime2))
                    .getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        // 毫秒ms
        long diff = t1 - t2;
        /*
         * long diffSeconds = diff / 1000 % 60; long diffMinutes = diff / (60 *
         * 1000) % 60; long diffHours = diff / (60 * 60 * 1000) % 24;
         */
        /*
         * long diffDays = diff / (24 * 60 * 60 * 1000);
         * 如果diff 为负数 long diffDays = diff / (24 * 60 * 60 * 1000) 算法 为0天
         * 实现为-1天,即，相差为负天数，diffDays要-1。
         */
        long diffDays = 0;
        if (diff >= 0) {
            diffDays = diff / (24 * 60 * 60 * 1000);
        } else {
            diffDays = diff / (24 * 60 * 60 * 1000) - 1;
        }
        return diffDays;
    }
    
    /**
     * 计算两个日期的时间差(天数、自然日)<br>
     * 上面那个getTimeDifference()有点问题，当时间差小于24小时时，会计算成0天，<br>
     * 比如今天中午12点，到明天早上10点，时间差小于24小时，用上面那个方法会算成0天。
     * 这样的话，可能不适用于一些业务。 (2014-12-27)
     * @param time1
     * @param time2
     * @return time1-time2的天数
     * @author liangyi
     */
    public static long getDatesDifference(Timestamp time1,Timestamp time2) {
        try {
            //把两个时间转化为同一整点,以计算准确的天数
            String time1Str=getDateString(time1, YYYYMMDD)+"000000";
            String time2Str=getDateString(time2, YYYYMMDD)+"000000";
            
            time1=getTimestamp(time1Str, YYYYMMDDHHMMSS);
            time2=getTimestamp(time2Str,YYYYMMDDHHMMSS);
            
            long t1 = time1.getTime();
            long t2 = time2.getTime();
            // 毫秒ms
            long diff = t1 - t2;
           
            long diffDays = diff / (24 * 60 * 60 * 1000);
            return diffDays;
        } catch (Exception e) {
            throw new RuntimeException("系统计算两个日期时间差时出错！",e);
        }
    }
    
    public static void main(String[] args) {
        String time1Str="201402010000000";
        String time2Str="20140301100000";
        
        Timestamp time1=getTimestamp(time1Str, YYYYMMDDHHMMSS);
        Timestamp time2=getTimestamp(time2Str,YYYYMMDDHHMMSS);
        
        System.out.println(getDatesDifference(time1,time2));
    }

    /**
     * 格式化时间 Locale是设置语言敏感操作
     * 
     * @param formatTime
     * @return
     */
    public static String getTimeStampNumberFormat(Timestamp formatTime) {
        SimpleDateFormat m_format = new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss",
                new Locale("zh", "cn"));
        return m_format.format(formatTime);
    }

    public static int getMillis() {
        Timestamp currTimestamp = DateUtil.getSysDate();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currTimestamp);
        return calendar.get(Calendar.MILLISECOND);
    }

    /**
     * 当前时间前一秒
     * 
     * @param currentDate
     * @return
     * @author zhangxianwei
     */
    public static Timestamp getBeforeSecond(Timestamp currentDate) {
        Calendar calender = Calendar.getInstance();
        calender.setTime(currentDate);
        calender.add(Calendar.SECOND, -1);
        return new Timestamp(calender.getTimeInMillis());
    }

    /**
     * 当前时间后一秒
     * 
     * @param currentDate
     * @return
     * @author zhangxianwei
     */
    public static Timestamp getAfterSecond(Timestamp currentDate) {
        Calendar calender = Calendar.getInstance();
        calender.setTime(currentDate);
        calender.add(Calendar.SECOND, 1);
        return new Timestamp(calender.getTimeInMillis());
    }

    public static Timestamp getTimestamp(String time) {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setLenient(false);
        Timestamp ts = null;
        try {
            ts = new Timestamp(format.parse(time).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ts;
    }

    /**
     * 将指定格式的日期字符串转成Timestamp
     * 
     * @param time
     * @param pattern
     * @return
     * @author mayt
     */
    public static Timestamp getTimestamp(String time, String pattern) {
        DateFormat format = new SimpleDateFormat(pattern);
        format.setLenient(false);
        Timestamp ts = null;
        try {
            ts = new Timestamp(format.parse(time).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ts;
    }

    /**
     * 计算两个日期的时间差(月)
     * 
     * @param formatTime1
     * @param formatTime2
     * @return
     * @author zhangxd7
     */
    public static int getTimeDifferenceMonth(Timestamp formatTime1,
            Timestamp formatTime2) {

        Date d1 = DateUtil.getTheDayDate(formatTime1);
        Date d2 = DateUtil.getTheDayDate(formatTime2);
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(d1);
        c2.setTime(d2);
        int y = c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR);// 年差
        int m = c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH);// / 月差
        int d = c2.get(Calendar.DAY_OF_MONTH) - c1.get(Calendar.DAY_OF_MONTH);// 天差

        if (d > 0) {
            // 如果天数差大于零
            return (y * 12 + m + 1);
        } else {
            return (y * 12 + m);
        }
    }
    
    /**
     * 用于判断 待验证的时间（checkTime）是否在 startTime 和  endTime 之间；是，那么返回 true; 否则：返回 false;
     * @param startTime
     * @param endTime
     * @param checkTime
     * @return
     * @author yugn
     */
    public static boolean isBetweenTime(Timestamp startTime, Timestamp endTime, Timestamp checkTime){
        
        //如果checkTime 在当前 startTime 和 endTime 之间，那么返回 true;
        if(checkTime.compareTo(startTime)>=0 && checkTime.compareTo(endTime)<=0){
            return true;
        }
        
        return false;
    }
    /**
     * 获得参数时间的周一时间.如果参数为空null,那么返回当前系统时间的周一日期
     * @param time
     * @return
     * @author huangjx
     */
    public static Timestamp getMondayDate(Timestamp time){
         Calendar cal =Calendar.getInstance();
            if(null!=time ){
                cal.clear();
                Date d = DateUtil.getTheDayDate(time);
                cal.setTime(d);
            }
             cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY); //获取本周一的日期
            // System.out.println("********得到本周一的日期*******"+df.format(cal.getTime()));
             return new Timestamp(cal.getTimeInMillis());
    }
    /**
     * 获得参数时间的周日时间.如果参数为空null,那么返回当前系统时间的周日日期
     * @param time
     * @return
     * @author huangjx
     */
    public static Timestamp getSundayDate(Timestamp time){
         Calendar cal =Calendar.getInstance();
            if(null!=time ){
                cal.clear();
                Date d = DateUtil.getTheDayDate(time);
                cal.setTime(d);
            }
           //这种输出的是上个星期周日的日期，因为老外那边把周日当成第一天
           cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
           //增加一个星期，才是我们中国人理解的本周日的日期
           cal.add(Calendar.WEEK_OF_YEAR, 1);
           return new Timestamp(cal.getTimeInMillis());
    }
    /**
     * 
     * addDays:加减月份天数. <br/> 
     * 
     * @author jiangysh 
     * @param date
     * @param daysOfMonth
     * @return 
     * @since JDK 1.6
     */
    public static java.util.Date addDays(Date date,int daysOfMonth){
        Calendar cal=Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, daysOfMonth);
        
        return cal.getTime();
    }
}

