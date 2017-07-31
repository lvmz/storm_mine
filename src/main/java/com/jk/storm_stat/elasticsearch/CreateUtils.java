package com.jk.storm_stat.elasticsearch;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created by lmz on 2017/7/28.
 */
public class CreateUtils {
    static String ipAddress  = "127.0.0.1";
    private static Settings settings = Settings.settingsBuilder()
            .put("cluster.name", "elasticsearch")
            .put("client.transport.sniff", true)
            .build();
    public static void main(String[] args) {
        try {
            /* 创建客户端 */
            // client startup

            Client client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ipAddress), 9300));

            List<String> jsonData = DataFactory.getInitJsonData();

            for (int i = 0; i < jsonData.size(); i++) {
                IndexResponse response = client.prepareIndex("blog", "article",""+i).setSource(jsonData.get(i)).get();
                if (response.isCreated()) {
                    System.out.println("创建成功!");
                }
            }

            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

