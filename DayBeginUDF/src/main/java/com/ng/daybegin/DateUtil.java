package com.ng.daybegin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    /**
     * 得到指定时间date的零时刻
     * @param d
     * @return
     */
    public static Date getDayBeginTime(Date d){
        // 通过SimpleDateFormat获得指定日期的零时刻
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
        try {
            return sdf.parse(sdf.format(d) );
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 得到指定date的偏移量零时刻
     * @param d
     * @param offset
     * @return
     */
    public static Date getDayBeginTime(Date d,int offset){
        try {
            // 通过SimpleDateFormat获得指定日期的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            Date beginDate = sdf.parse(sdf.format(d));
            Calendar c = Calendar.getInstance();
            c.setTime(beginDate);
            // 通过Calendar实例实现按照天数偏移
            c.add(Calendar.DAY_OF_MONTH,offset);

            return c.getTime();
        } catch (ParseException e){
            e.printStackTrace();
        }
        return  null;
    }

    /**
     * 得到指定date所在周的起始零时刻
     * @param date
     * @return
     */
    public static Date getWeekBeginTime(Date date){
        //得到指定日期d的零时刻
        Date beginDate = getDayBeginTime(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(beginDate);
        //得到当前日期是所在周的第几天
        int n = calendar.get(Calendar.DAY_OF_WEEK);
        //通过减去周偏移量得到本周的第一天
        calendar.add(Calendar.DAY_OF_MONTH, -(n-1));
        return calendar.getTime();
    }

    /**
     * 得到距离指定date所在周offset周之后的一周的起始时刻
     * @param date
     * @param offset
     * @return
     */
    public static Date getWeekBeginTime(Date date, int offset){
        //得到d的零时刻
        Date beginDay = getDayBeginTime(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(beginDay);

        int n = calendar.get(Calendar.DAY_OF_WEEK);
        //定位到本周第一天
        calendar.add(Calendar.DAY_OF_MONTH, -(n-1));
        //通过Calendar类实现按周进行偏移
        calendar.add(Calendar.DAY_OF_MONTH , offset*7);

        return calendar.getTime();
    }

    /**
     * 得到指定date所在月的起始时刻
     * @param d
     * @return
     */
    public static Date getMonthBeginTime(Date d){
        try {
            //得到d的零时刻
            Date beginDate = getDayBeginTime(d);
            //得到date所在月的第一天的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

            return sdf.parse(sdf.format(beginDate));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 得到距离指定date所在月offset个月之后的月的起始时刻
     * @param d
     * @param offset
     * @return
     */
    public static Date getMonthBeginTime(Date d, int offset){
        //得到d的零时刻
        Date beginDate = getDayBeginTime(d);
        //得到date所在月的第一天的零时刻
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

        try {
            //d所在月的第一天的零时刻
            Date firstDay = sdf.parse(sdf.format(beginDate));
            //通过Calendar实现按月进行偏移
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(firstDay);

            calendar.add(Calendar.MONTH, offset);
            return calendar.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
