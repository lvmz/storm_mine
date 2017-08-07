package com.jk.storm_stat.hbaseTest;

import com.jk.storm_stat.util.PropertiesType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * Created by lmz on 2017/8/7.
 */
public class HbaseTest {
    public static void main(String[] args) {
        String tableName = "lv_test";
        String[] family = {"identity","assess"};
        try {
            HbaseUtils util = new HbaseUtils(PropertiesType.JK_HBASE_TEST, tableName);
            //建表
            util.creatTable(tableName, family);
            util.addData("lvmz1","sex","human");
            //单个增加
            Put put = new Put(Bytes.toBytes("lvmz2"));
            put.add(Bytes.toBytes("identity"), Bytes.toBytes("age"), Bytes.toBytes("18"));

            //批增加
            Put put3 = new Put(Bytes.toBytes("lvmz3"));
            put3.add(Bytes.toBytes("identity"), Bytes.toBytes("age"), Bytes.toBytes("18"));
            Put put4 = new Put(Bytes.toBytes("lvmz4"));
            put4.add(Bytes.toBytes("identity"), Bytes.toBytes("age"), Bytes.toBytes("18"));
            ArrayList<Put> puts = new ArrayList<>();
            puts.add(put3);
            puts.add(put4);
            util.addDataBatch(puts);
            //各种查询
            util.queryByRowKey("lvmz1","lvmz2");

            util.queryAll();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
