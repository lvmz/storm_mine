package com.jk.storm_stat.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author ThinkPad User
 *
 */
public class UtilTools {
	
	/*
	 * 时间戳转化
	 */
	public static String timeStamp2Date(Long timeMillis,String format) {
		if(timeMillis == null || timeMillis.equals("null")){
			return "";
		}
		String seconds = String.valueOf(timeMillis / 1000);
        if(seconds == null || seconds.isEmpty() || seconds.equals("null")){  
            return "";  
        }  
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd HH:mm:ss";  
        SimpleDateFormat sdf = new SimpleDateFormat(format);  
        return sdf.format(new Date(Long.valueOf(seconds+"000")));  
    }
	
	public static String timeStamp2Day(Long timeMillis,String format) {
		if(timeMillis == null || timeMillis.equals("null")){
			return "";
		}
		String seconds = String.valueOf(timeMillis / 1000);
        if(seconds == null || seconds.isEmpty() || seconds.equals("null")){  
            return "";  
        }  
		if (format == null || format.isEmpty())
			format = "yyyy-MM-dd";  
        SimpleDateFormat sdf = new SimpleDateFormat(format);  
        return sdf.format(new Date(Long.valueOf(seconds+"000")));  
    }
	
	public static String getDay(Long timeMillis,String format) {
		if(timeMillis == null || timeMillis.equals("null")){
			return "";
		}
		String seconds = String.valueOf(timeMillis / 1000);
        if(seconds == null || seconds.isEmpty() || seconds.equals("null")){  
            return "";  
        }  
		if (format == null || format.isEmpty())
			format = "dd";
        SimpleDateFormat sdf = new SimpleDateFormat(format);  
        return sdf.format(new Date(Long.valueOf(seconds+"000")));  
    }
	
	public static void main(String[] args) {
		System.out.println(UtilTools.getDay(System.currentTimeMillis(),null));
	}
	
	/**
     * 根据指定延迟天数，获取unixTime时间戳
     * @param day
     * @return
     */
    public static long getUnixTime(int day){
        Calendar calendar = Calendar.getInstance();
        //设置延迟天数
        calendar.add(Calendar.DAY_OF_MONTH, day);
        //设置时
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        //设置分
        calendar.set(Calendar.MINUTE, 59);
        //设置秒
        calendar.set(Calendar.SECOND, 59);
        //设置毫秒
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis()/1000;
    }
	
}
