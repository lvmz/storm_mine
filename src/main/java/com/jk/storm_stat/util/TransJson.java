package com.jk.storm_stat.util;


import com.jk.storm_stat.bean.ColumnsBean;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;

public class TransJson {
	private static final Logger EXCEPTATALOG = LoggerFactory
			.getLogger("exceptMessage");
	private static JSONObject josnObject = new JSONObject();
	private static String[] values = null;
	private static String[] keyValues = null;
	private static String[] contentValues = null;

	/**
	 * 将从loghub上的埋点日志转换成json格式
	 *
	 * @return
	 */
	public static String toJson(String str) {
		try {
			values = str.split("\\|");
			josnObject.clear();
			josnObject.put("uuid", UUID.randomUUID().toString()
					.replace("-", ""));
			josnObject.put("kafkaTime", System.currentTimeMillis());
			for (String s : values) {
				keyValues = s.split(":");
				if (!keyValues[1].startsWith("{")) {
					if (keyValues.length > 2) {
						josnObject.put(keyValues[0],
								s.split(keyValues[0] + ":")[1]);
					} else {
						josnObject.put(keyValues[0], keyValues[1]);
					}
				} else {
					contentValues = s.split("\\:\\{");
					if (contentValues[1].length() != 1) {
						josnObject
								.put(contentValues[0], "{" + contentValues[1]);
					}
				}
			}
			return josnObject.toString();

		} catch (Exception e) {
			// System.out.println("except data:" + str);
			// 写入异常数据日志
			EXCEPTATALOG.info("except data:" + str);
		}
		return null;
	}

	public static void flattenJson(JSONObject oldJson, JSONObject newJson) {
		Iterator it = oldJson.keys();
		while (it.hasNext()) {
			String key = it.next().toString();
			Object object = oldJson.get(key);
			if (object instanceof JSONObject) {
				flattenJson((JSONObject) object, newJson);
			} else {
				if (object.toString().equals("null")) {
					newJson.put(key.toLowerCase(), "");
				} else {
					newJson.put(key.toLowerCase(), object.toString());
				}
			}
		}
	}

	public static List<ColumnsBean> parseXml() {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder;
		List<ColumnsBean> keys = new ArrayList<ColumnsBean>();
		try {
			documentBuilder = factory.newDocumentBuilder();
			Document doc = documentBuilder.parse(TransJson.class
					.getClassLoader().getResourceAsStream("log_parent.xml"));
			NodeList nodeList = doc.getElementsByTagName("field");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				ColumnsBean columnBean = new ColumnsBean();
				columnBean.name = node.getAttributes().getNamedItem("name")
						.getTextContent();
				columnBean.type = node.getAttributes().getNamedItem("type")
						.getTextContent();
				columnBean.isnull = Boolean.parseBoolean(node.getAttributes()
						.getNamedItem("isnull").getTextContent());
				columnBean.defaults = node.getAttributes()
						.getNamedItem("defaults").getTextContent();
				keys.add(columnBean);
			}
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return keys;
	}

	public static Map<String, String> getColumnsAndValues() {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder;
		Map<String, String> columnAndValues = new HashMap<String, String>();
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();
		try {
			documentBuilder = factory.newDocumentBuilder();
			Document doc = documentBuilder.parse(TransJson.class
					.getClassLoader().getResourceAsStream("log_parent.xml"));
			NodeList nodeList = doc.getElementsByTagName("field");
			for (int i = 0; i < nodeList.getLength(); i++) {
				columns.append(
						nodeList.item(i).getAttributes().getNamedItem("name")
								.getTextContent()).append(",");
				values.append("?").append(",");
			}
			// 追加数据入库时间
			columns.append("inserttime");
			values.append("FROM_UNIXTIME('"
					+ (System.currentTimeMillis() / 1000)
					+ "','%Y-%c-%d %h:%i:%s')");
			columnAndValues.put("columns", columns.toString());
			columnAndValues.put("values", values.toString());
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return columnAndValues;
	}

	/**
	 * main
	 * @param args
	 */
//	public static void main(String[] args) {
//
//		ConnectionPool pool = ConnectionPool.getInstance();// 获取数据库连接池
//		SimpleDateFormat dateformat = new SimpleDateFormat(
//				"yyyy-MM-dd hh:mm:ss");// 指定String转日期格式
//
//		String message = "{\"uuid\":\"a6c95b937dd04c2699c22777d464abc1\",\"kafkaTime\":1495592190238,\"logLevel\":\"INFO\",\"userId\":\"1379102\",\"userName\":\"华艺灯光王玉龙17352322111\",\"userip\":null,\"createtime\":\"2017-05-26 10:16:27\",\"sessionid\":\"oudaAs4-AHKzU65Djp7CUXiRL4Lk\",\"useend\":\"31\",\"eventtype\":\"jk_pageview\",\"functionname\":\"查看提分计划首页\",\"useragreement\":\"https\",\"netstatus\":\"200\",\"cid\":\"江苏省盐城市毓龙路实验学校（初中）\",\"ctype\":\"学校\",\"cururl\":\"https://parent.fclassroom.com/improvingScore_index\",\"position\":\"江苏省盐城市\",\"phonepara\":{\"phoneversion\":\"iPhone\",\"imei\":\"Mobile Safari\",\"appkey\":\"iOS\"},\"content\":{\"modulename\":\"提分计划首页-极课家长-极课大数据\",\"versionnum\":\"2.4\",\"subjectid\":68317,\"subjectname\":null,\"studentid\":1228087,\"frompage\":null,\"pid\":1379102,\"mobile\":\"17352322111\"}}";
////
////		String me = message.replaceAll("\\\\", "\\");
////		String me = StringEscapeUtils.unescapeJava(message);
//		System.out.println("-------------" + message);
//		JSONObject jsonObject = JSONObject.fromObject(message);
//		// createtime数据校验
//		if (!CheckUtils.checkCreateTime(jsonObject)) {
//			System.out.println("check createtime fail" + message);
//			return;
//		}
//
//		// 创建新的Json对象
//		JSONObject newJson = new JSONObject();
//		// 扁平化json格式
//		TransJson.flattenJson(jsonObject, newJson);
//		// 解析xml，获取属性列表
//		List<ColumnsBean> columnBeanList = TransJson.parseXml();
//		// 获取拼装完成的columns列表，values占位列表
//		Map<String, String> columnsAndValues = TransJson.getColumnsAndValues();
//		// 拼装sql
//		String sql = "insert into log_parent("
//				+ columnsAndValues.get("columns") + ") values("
//				+ columnsAndValues.get("values") + ")";
//		Connection conn = null;
//		PreparedStatement ps = null;
//		try {
//			// 获取数据库链接
//			conn = pool.getConnection();
//			ps = conn.prepareStatement(sql);
//			// 遍历属性集合，获取json对象中对应的值
//			for (int i = 0; i < columnBeanList.size(); i++) {
//				Object object = newJson.get(columnBeanList.get(i).name);
//				// 判断属性值是否为空
//				if (null != object && StringUtils.isNotBlank(object.toString())) {
//					if (columnBeanList.get(i).type.equals("int")) {
//						ps.setLong((i + 1), Long.parseLong(object.toString()));
//					} else if (columnBeanList.get(i).type.equals("string")) {
//						ps.setString((i + 1), object.toString());
//					} else if (columnBeanList.get(i).type.equals("date")) {
//						java.util.Date date = dateformat.parse(object
//								.toString());
//						ps.setTimestamp((i + 1), new Timestamp(date.getTime()));
//					}
//				} else {// 属性值为空，set对应的空值
//					// 属性是否可以为空
//					if (columnBeanList.get(i).isnull) {
//						// 没有有默认值
//						if (StringUtils.isBlank(columnBeanList.get(i).defaults)) {
//							if (columnBeanList.get(i).type.equals("int")) {
//								ps.setNull((i + 1), Types.BIGINT);
//							} else if (columnBeanList.get(i).type
//									.equals("string")) {
//								ps.setNull((i + 1), Types.VARCHAR);
//							} else if (columnBeanList.get(i).type
//									.equals("date")) {
//								ps.setNull((i + 1), Types.DATE);
//							}
//						} else {// 有默认值
//							if (columnBeanList.get(i).type.equals("int")) {
//								ps.setLong(
//										(i + 1),
//										Long.parseLong(columnBeanList.get(i).defaults));
//							} else if (columnBeanList.get(i).type
//									.equals("string")) {
//								ps.setString((i + 1),
//										columnBeanList.get(i).defaults);
//							} else if (columnBeanList.get(i).type
//									.equals("date")) {
//								java.util.Date date = dateformat
//										.parse(columnBeanList.get(i).defaults);
//								ps.setTimestamp((i + 1),
//										new Timestamp(date.getTime()));
//							}
//						}
//					} else {
//						System.out.println(columnBeanList.get(i).name
//								+ "：不能为空！log:" + message);
//						return;
//					}
//				}
//
//			}
//			if (ps.executeUpdate() == 0) {
//				// 保存失败
//				System.out.println("保存失败！！");
//			} else {
//				System.out.println("保存成功！！！");
//			}
//
//		} catch (Exception e) {
//			// 异常处理
//			e.printStackTrace();
//		} finally {
//			try {
//				ps.close();
//				conn.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//	}
}
