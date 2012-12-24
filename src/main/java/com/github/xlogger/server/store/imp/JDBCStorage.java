package com.github.xlogger.server.store.imp;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


import com.github.xlogger.common.XLoggerData;
import com.github.xlogger.server.store.Storage;

public class JDBCStorage implements Storage, Runnable {

	private Connection conn;
	
	private static ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setDaemon(true);
			return thread;
		}
	});
	
	private Object writeLock = new Object();
	
	private static final List<JDBCStorage> myIdleList = new LinkedList<JDBCStorage>();
	
	public JDBCStorage(String jdbcDriver, String url, String user, String password) throws SQLException, ClassNotFoundException {
		Class.forName(jdbcDriver);
		conn = DriverManager.getConnection(url, user, password);
		myIdleList.add(this);
		service.schedule(this, 3, TimeUnit.MINUTES);
	}
	
	private long lastWroteTime;
	
	@Override
	public void save(XLoggerData data) throws Exception {
		String sql = data.toSQL();
		PreparedStatement ps = conn.prepareStatement(sql);
		try {
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
		
			synchronized (writeLock) {
				if (ps.executeUpdate() == 0) {
					throw new Exception("no record inserted (`" + data.getTabName() + "`");
				}
			}
		} finally {
			ps.close();
		}
		lastWroteTime = System.currentTimeMillis();
	}
	
	private void sendPing() throws SQLException {
		PreparedStatement ps = conn.prepareStatement("SELECT CURRENT_TIME()");
		try {
			synchronized (writeLock) {
				ps.execute();
			}
			lastWroteTime = System.currentTimeMillis();
		} finally {
			ps.close();
		}
	}

	@Override
	public void run() {
		for (JDBCStorage storage: myIdleList) {
			if (System.currentTimeMillis() - storage.lastWroteTime >= 60000) {
				try {
					storage.sendPing();
				} catch (SQLException e) {
				}
			}
		}
	}

}
