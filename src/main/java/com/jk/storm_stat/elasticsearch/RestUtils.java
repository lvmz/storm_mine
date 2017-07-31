package com.jk.storm_stat.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class RestUtils {
	public static final String CLUSTER_NAME = "elasticsearch"; // 实例名称
	private static final String IP1 = "127.0.0.1";
	private static final int PORT = 9300; // 端口
	// 1.设置集群名称：默认是elasticsearch，并设置client.transport.sniff为true，使客户端嗅探整个集群状态
	// 把集群中的其他机器IP加入到客户端，对ES2.0有效
	private static Settings settings = Settings.settingsBuilder()
			.put("cluster.name", CLUSTER_NAME)
			.put("client.transport.sniff", true)
			.build();

	// 创建私有对象
	private static TransportClient client;

	static {
		try {
			client = TransportClient
					.builder()
					.settings(settings)
					.build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP1), PORT));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/*
	 * 取得实例
	 */
	public static TransportClient getTransportClient() {
		return client;
	}

	/*
	 * 根据id获取
	 */
	public static void getById() {
		GetResponse getResponse = client.prepareGet("blog", "article", "1").get();
		System.out.println(getResponse.getSourceAsString());
	}

	/*
	 * 查询所有数据
	 */
	public static SearchHit[] getAll(String indexStr, String typeStr) {
		String index = indexStr;//"sbstorage";
		String type = typeStr;//"2017-07-25tmp";//时间
		SearchResponse searchResponse = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())// 查询所有
				.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom(0)
				.setSize(2000)// 分页
				.get();

		SearchHits hits = searchResponse.getHits();
		long total = hits.getTotalHits();
		System.out.println(total);
		SearchHit[] searchHits = hits.hits();
		return searchHits;
	}

	/*
	 * 查询所有数据
	 */
	public static void getAll1() {
		String index = "blog";
		String type = "article";//时间
		SearchResponse searchResponse = client
				.prepareSearch(index)
				.setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery())// 查询所有
				.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom(0)
				.setSize(100)// 分页
				.get();

		SearchHits hits = searchResponse.getHits();
		long total = hits.getTotalHits();
		System.out.println(total);
		SearchHit[] searchHits = hits.hits();
		for (SearchHit s : searchHits) {
            System.out.println(s.getId());
//			System.out.println(s.getSource().get("id"));
			System.out.println(s.getSourceAsString());
			String[] logindex = s.getSourceAsString().split(",");
		}
	}

	public static void main(String[] args) {
		// testGet();
//		getAll1();
//		readFromES("sbstorage","2017-07-25tmp");
//		readEsOfAll("","","");
        getAll1();
	}

	public static void close() {
		client.close();
	}

}