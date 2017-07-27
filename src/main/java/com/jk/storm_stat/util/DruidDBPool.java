package com.jk.storm_stat.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public final class DruidDBPool {
	private DataSource ds = null;
	private Connection connection = null;
	private PropertiesType properties = null;
	private PreparedStatement prepstmt = null;
	private boolean error = false;

	private static final Logger LOG = LoggerFactory.getLogger(DruidDBPool.class);
	
	public DruidDBPool(PropertiesType type){
		properties = type;
		try {
			InputStream in = DruidDBPool.class.getClassLoader().getResourceAsStream(
					properties.getValue());
			Properties props = new Properties();
			props.load(in);
			//此处会创建多个线程,只初始化一次
			ds = DruidDataSourceFactory.createDataSource(props);

			if(ds == null) {
				System.out.println("DruidDBPool 初始化错误");
			}
		} catch (Exception e) {
			LOG.error("getConnection error:" + e.getMessage());
		}
	}
	/*
	 * get connection and return a Connection object
	 */
	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

	public void execute(final String sql) throws SQLException {
	    if(this.connection == null){
	        try {
	            connection = this.getConnection();
	        } catch (SQLException e) {
	            LOG.error("execute:" + e.getMessage());
	        }
	    }

		if (this.connection == null) {
		    LOG.error("execute:conn get failed. drop sql:" + sql);
			return;
		}
		
	    try {
	    	prepstmt = this.connection.prepareStatement(sql);
			prepstmt.executeUpdate();
            if(prepstmt!=null){
                prepstmt.close();
            }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOG.error("execute:execute failed. message = " + e.getMessage()+" drop sql:" + sql);
			this.error = true;
		}finally {
            if(prepstmt!=null){
                prepstmt.close();
            }
        }
        if(this.error){
	    	try {
	    		this.connection.close();
	    		this.connection = null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				LOG.error("execute:conn.close failed. message = " + e.getMessage());
			}
	    }
	    this.error = false;
	}
	
	 /**
     * 执行sql获取结果
     * @param sql
     * @return
     */
    public Set<String> executeSql4Result(final String sql){
        Set<String> roomset = new HashSet<String>();
        ResultSet rs = null;
        if(StringUtils.isNotBlank(sql)){
            if(this.connection == null){
                try {

                    connection = this.getConnection();
                } catch (SQLException e) {
                    LOG.error("execute:" + e.getMessage());
                }
            }

            if (this.connection == null) {
                LOG.error("execute:conn get failed. drop sql:" + sql);
                return null;
            }
            
            try {

                prepstmt = this.connection.prepareStatement(sql);
                rs = prepstmt.executeQuery(sql);
				while(rs.next()) {
                    roomset.add(rs.getString("pk_room"));
				}
				if(prepstmt!=null){
				    prepstmt.close();
				}
                if(rs!=null){
                    rs.close();
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                LOG.error("execute:execute failed. message = " + e.getMessage()+" drop sql:" + sql);
                this.error = true;
            }
        }
        
        if(this.error){
            try {
                this.connection.close();
                this.connection = null;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                LOG.error("execute:conn.close failed. message = " + e.getMessage());
            }
        }
        this.error = false;

        return roomset;
    }
}
