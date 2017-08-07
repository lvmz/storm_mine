package com.jk.storm_stat.hbaseTest;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lmz on 2017/8/7.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.jk.storm_stat.util.PropertiesType;
import com.jk.storm_stat.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HbaseUtils {
    private Configuration conf = null;
    HTable table = null;
    private String tableName;

    private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);

    public HbaseUtils(PropertiesType propertiesType, String tableName) {
        conf = HBaseConfiguration.create();
        try {

            if (propertiesType.getValue().contains("online")) {
                tableName = "lf:" + tableName;
            }

            PropertiesUtil properties = new PropertiesUtil(propertiesType.getValue());

            conf.set("hbase.zookeeper.quorum", properties.getValue("hbase.zookeeper.quorum"));
            conf.set("hbase.zookeeper.property.clientPort", properties.getValue("hbase.zookeeper.property.clientPort"));

            String[] arr = new String[] { "hbase.master.kerberos.principal", "hbase.regionserver.kerberos.principal", "hbase.security.authentication", "hadoop.security.authentication", "hbase.rpc.protection" };

            for (String key : arr) {
                String value = properties.getValue(key);
                if (value != null && !"".equals(value)) {
                    conf.set(key, value);
                }
            }

            UserGroupInformation.setConfiguration(conf);

            if (properties.getValue("keytab") != null && !"".equals(properties.getValue("keytab"))) {
                UserGroupInformation.loginUserFromKeytab("yule/hbaseclient@HBASE.YOUKU", properties.getValue("keytab"));
            }

            table = new HTable(conf, Bytes.toBytes(tableName));
        } catch (Exception e) {
            LOG.error("getConnection error:" + e.getMessage());
        }

        this.tableName = tableName;
    }

    /**
     * 添加数据
     *
     * @param rowKey
     * @param column
     * @param value
     */
    public void addData(String rowKey, String column, Object value) {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(column), Bytes.toBytes(column), Bytes.toBytes(value.toString()));
            table.put(put);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * 添加数据
     */
    public void addData(Put put) {
        try {
            table.put(put);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * 批量添加数据
     *
     * @param list
     */
    public void addDataBatch(List<Put> list) {
        try {
            table.put(list);
        } catch (RetriesExhaustedWithDetailsException e) {
            LOG.error(e.getMessage());
        } catch (InterruptedIOException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     *            表名
     * @param family
     *            列名
     * @throws Exception
     */
    public void creatTable(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            HColumnDescriptor columnDesc = new HColumnDescriptor(family[i]);
            desc.addFamily(columnDesc);
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    public void dropTable() {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 根据rowkey查询
     *
     * @param start
     * @param end
     * @return
     */
    public ResultScanner queryByRowKey(String start, String end) {
        try {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(start));
            scan.setStopRow(Bytes.toBytes(end));

            Filter filter = new PageFilter(20);
            return table.getScanner(scan);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public List<Map<String, String>> query(FilterList filterlist, byte[] startRow, byte[] stopRow) {

        List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();

        try {
            Scan scan = new Scan();
            scan.setFilter(filterlist);

            if (startRow != null) {
                scan.setStartRow(startRow);
            }
            if (stopRow != null) {
                scan.setStopRow(stopRow);
            }

            ResultScanner results = table.getScanner(scan);

            List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
            for (Result result : results) {

                Map<String, String> map = new HashMap<String, String>();
                for (Cell cell : result.rawCells()) {
                    String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    map.put(qualifier, value);
                }
                resultList.add(map);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultList;
    }

    /**
     * 根据rowkey分页查询
     *
     * @param start
     * @param end
     * @return
     */
    public ResultScanner queryByRowKeyInPage(String start, String end, Integer pageSize) {
        try {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(start));
            scan.setStopRow(Bytes.toBytes(end));
            Filter pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);
            ResultScanner results = table.getScanner(scan);

            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 查询全部
     */
    public void queryAll() {
        Scan scan = new Scan();
        try {
            ResultScanner results = table.getScanner(scan);
            int rowCount = 0;
            for (Result result : results) {
                int i = 0;
                for (KeyValue rowKV : result.list()) {

//					if (i++ == 0) {
//						System.out.print( "rowkey:" + new String(rowKV.getRow()) + " ");
//					}
//					System.out.print(" " + new String(rowKV.getQualifier()) + " ");
//					System.out.print(":" + new String(rowKV.getValue()));

                }

//				System.out.println();
                rowCount++;
            }

            System.out.println("rowCount:" + rowCount);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

    }

    /**
     * 按某字段查询 column = value 的数据
     *
     * @param queryColumn
     *            要查询的列名
     * @param value
     *            过滤条件值
     * @param columns
     *            返回的列名集合
     */
    public ResultScanner queryBySingleColumn(String queryColumn, String value, String[] columns) {
        if (columns == null || queryColumn == null || value == null) {
            return null;
        }

        try {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(queryColumn), Bytes.toBytes(queryColumn), CompareOp.EQUAL, new SubstringComparator(value));
            Scan scan = new Scan();

            for (String columnName : columns) {
                scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
            }

            scan.setFilter(filter);
            return table.getScanner(scan);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        return null;
    }

    /**
     * 根据对比条件，按某字段查询 column = value 的数据
     *
     * @param queryColumn
     *            要查询的列名
     * @param value
     *            过滤条件值
     * @param columns
     *            返回的列名集合
     * @param op
     *            操作符号 CompareOp.EQUAL 代表等于,CompareOp.LESS代表小于
     */
    public ResultScanner queryBySingleColumnOnCompareOp(String queryColumn, String value, String[] columns, CompareOp op) {
        if (columns == null || queryColumn == null || value == null) {
            return null;
        }

        try {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(queryColumn), Bytes.toBytes(queryColumn), op, new SubstringComparator(value));
            Scan scan = new Scan();

            for (String columnName : columns) {
                scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
            }

            scan.setFilter(filter);
            return table.getScanner(scan);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        return null;
    }

    public void getRegionInfo() {
        try {
            NavigableMap<HRegionInfo, ServerName> map = table.getRegionLocations();
            for (Map.Entry<HRegionInfo, ServerName> entry : map.entrySet()) {
                HRegionInfo info = entry.getKey();
                ServerName name = entry.getValue();

                System.out.println(info + " __________ " + name);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 在指定的条件下，按某一字段聚合
     *
     * @param paramMap
     *            参数条件
     * @param dimensionColumns
     *            维度
     * @param aggregateColumn
     *            聚合字段
     * @return 返回map，key 为dimensionColumns 维度相对应的数据，value 为aggregateColumn
     *         字段对应的值
     */
    public Map<String, Long> aggregateBySingleColumn(Map<String, String> paramMap, String[] dimensionColumns, String aggregateColumn) {
        if (dimensionColumns == null || dimensionColumns.length == 0 || paramMap == null || aggregateColumn == null || aggregateColumn.equals("")) {
            return null;
        }

        Map<String, Long> map = null;
        HTable newTable = null;
        try {
            FilterList filterList = new FilterList();
            Scan scan = new Scan();
            // 添加过滤条件
            for (String paramKey : paramMap.keySet()) {
                SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(paramKey), Bytes.toBytes(paramKey), CompareOp.EQUAL, new SubstringComparator(paramMap.get(paramKey)));
                filterList.addFilter(filter);
            }
            scan.setFilter(filterList);

            // 要展现的列
            for (String column : dimensionColumns) {
                scan.addColumn(Bytes.toBytes(column), Bytes.toBytes(column));
            }
            scan.addColumn(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn));

            newTable = new HTable(conf, Bytes.toBytes(tableName));

            if (newTable != null) {
                ResultScanner results = newTable.getScanner(scan);

                // 将查询结果放入map 中
                map = new ConcurrentHashMap<String, Long>();
                for (Result result : results) {
                    // String dimensionKey = "";
                    StringBuilder dimensionKey = new StringBuilder();
                    // 取值
                    String value = new String(result.getValue(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn)));
                    Long aggregateValue = value == null ? 0 : Long.parseLong(value);

                    // 拼接Key
                    for (String column : dimensionColumns) {
                        dimensionKey.append("\t" + new String(result.getValue(Bytes.toBytes(column), Bytes.toBytes(column))));
                    }
                    dimensionKey = dimensionKey.deleteCharAt(0);

                    if (map.containsKey(dimensionKey)) {
                        map.put(dimensionKey.toString(), map.get(dimensionKey.toString()) + aggregateValue);
                    } else {
                        map.put(dimensionKey.toString(), aggregateValue);
                    }
                }

                results.close();
            }

            newTable.close();

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        return map;
    }

    public void testRowKeyQuery(String date) {
        ResultScanner results = queryByRowKey("-1_" + date + "_" + date + " 00:00:00_-1", "-1_" + date + "_" + date + " 23:59:59_-1");
        for (Result result : results) {
            int i = 0;
            for (KeyValue rowKV : result.list()) {

                if (i++ == 0) {
                    System.out.print("rowkey:" + new String(rowKV.getRow()) + " ");
                }
                System.out.print(" " + new String(rowKV.getQualifier()) + " ");
                System.out.print(":" + new String(rowKV.getValue()));

            }

            System.out.println();
        }
    }

    public void testqueryByRowKeyInPage(String date) {
        String startRowKey = "-1_" + date + "_" + date + " 00:00:00_-1";
        String endRowKey = "-1_" + date + "_" + date + " 23:59:59_-1";

        int page = 1;
        while (true) {
            ResultScanner results = queryByRowKeyInPage(startRowKey, endRowKey, 20);

            String rowkey = "";
            int totalRows = 0;

            System.out.println("第" + page + "页:");
            for (Result result : results) {
                int i = 0;
                for (KeyValue rowKV : result.list()) {

                    if (i++ == 0) {
                        System.out.print("rowkey:" + new String(rowKV.getRow()) + " ");
                        rowkey = new String(rowKV.getRow());
                    }
                    System.out.print(" " + new String(rowKV.getQualifier()) + " ");
                    System.out.print(":" + new String(rowKV.getValue()));

                }

                totalRows++;

                System.out.println();
            }

            results.close();

            if (totalRows == 0) {
                break;
            }
            startRowKey = rowkey;
            page++;
        }
    }

    /**
     * 根据 房间号和日期获取rowkey
     *
     * @param roomId
     * @param statDate
     * @return
     */
    public static String getRowKeyByRoom(Integer roomId, String statDate) {
        Integer mod = roomId % 10;
        String modStr = mod + "";
        if (mod < 10) {
            modStr = "0" + mod;
        }

        String rowkey = modStr + "_" + roomId + "_" + statDate;
        return rowkey;
    }

    public int getRowCount(String tableName, String column) {
        AggregationClient aggregationClient = new AggregationClient(conf);
        Scan scan = new Scan();
        //指定扫描列族，唯一值
        scan.addFamily(Bytes.toBytes(column));
        long rowCount = 0;
        try {
            rowCount = aggregationClient.rowCount(TableName.valueOf(tableName),  new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("row count is " + rowCount);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // String tableName = "lf:lf_t_result_room_online_min_user_stat";
        //
        // HbaseNewUtils util = new
        // HbaseNewUtils(PropertiesType.DDSHOW_HASE_TEST, tableName);
        //
        // String date = "2016-03-23";
        // System.out.println(date);
        // util.testRowKeyQuery(date);

        String tableName = "lf_t_view_hbase_room_stat";

        HbaseUtils util = new HbaseUtils(PropertiesType.JK_HBASE_TEST, tableName);

//		util.getRowCount(tableName, "roomId");


        Integer roomId = 263686;
        String statDate = "2016-07-08";

        String rowkey = getRowKeyByRoom(roomId, statDate);

        ResultScanner rs =  util.queryByRowKey(rowkey, rowkey);

        for (Result result : rs) {
            int i = 0;
            for (KeyValue rowKV : result.list()) {

                if (i++ == 0) {
                    System.out.print( "rowkey:" + new String(rowKV.getRow()) + " ");
                }
                System.out.print(" " + new String(rowKV.getQualifier()) + " ");
                System.out.print(":" + new String(rowKV.getValue()));

            }

            System.out.println();
        }
    }
}
