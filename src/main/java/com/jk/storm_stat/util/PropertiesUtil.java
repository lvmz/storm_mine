package com.jk.storm_stat.util;

import java.util.ResourceBundle;

/**
 * 
 * @author lvmengzheng
 *
 */
public class PropertiesUtil {
	ResourceBundle resourceBundle = null;
	
	public PropertiesUtil(String name) {
		resourceBundle = ResourceBundle.getBundle(name);
	}
	
	public String getValue(String key) {
		String result = "";
		if (resourceBundle.containsKey(key)) {
			result = resourceBundle.getString(key);
		}
		return result;
	}
}
