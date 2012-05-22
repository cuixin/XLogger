package com.github.xlogger.server.store.imp;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.github.xlogger.common.XLoggerData;
import com.github.xlogger.server.store.Storage;

public class JDBCStorage implements Storage {

	private Connection conn;
	
	public JDBCStorage(String jdbcDriver, String url, String user, String password) throws SQLException, ClassNotFoundException {
		Class.forName(jdbcDriver);
		conn = DriverManager.getConnection(url, user, password);
	}
	
	@Override
	public void save(XLoggerData data) throws Exception {
		String sql = data.toSQL();
		PreparedStatement ps = conn.prepareStatement(sql);
		Object[] args = data.getArgs();
		int i = 1;
		for (Object obj: args) {
			if (obj.getClass() == Byte.class) {
				ps.setByte(i, (Byte) obj);
			} else if (obj.getClass() == Integer.class) {
				ps.setInt(i, (Integer) obj);
			} else if (obj.getClass() == Boolean.class) {
				ps.setBoolean(i, (Boolean) obj);
			} else if (obj.getClass() == String.class) {
				ps.setString(i, (String) obj);
			} else if (obj.getClass() == Long.class) {
				ps.setLong(i, (Long) obj);
			} else if (obj.getClass() == Double.class) {
				ps.setDouble(i, (Double) obj);
			} else if (obj.getClass() == Float.class) {
				ps.setFloat(i, (Float) obj);
			} else if (obj.getClass() == Timestamp.class) {
				ps.setTimestamp(i, (Timestamp) obj);
			} else if (obj.getClass() == Date.class) {
				ps.setDate(i, (Date) obj);
			}
			i++;
		}
		
		if (ps.executeUpdate() == 0) {
			throw new Exception("no record inserted (`" + data.getTabName() + "`");
		}
	}

}
