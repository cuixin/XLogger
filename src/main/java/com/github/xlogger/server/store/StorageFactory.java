package com.github.xlogger.server.store;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.xlogger.common.XLoggerData;
import com.github.xlogger.server.store.imp.JDBCStorage;

public class StorageFactory {

	private static Storage[] storage = null;
	
	private static AtomicInteger workerId = new AtomicInteger();
	
	private static final int MAX_CPU = 15;
	
	synchronized public static boolean setupJDBC(String jdbcDriver, String url, String user, String passwd) {
		boolean initOk = false;
		if (storage == null) {
			storage = new JDBCStorage[MAX_CPU + 1];
			for (int i = 0; i <= MAX_CPU; i++) {
				try {
					storage[i] = new JDBCStorage(jdbcDriver, url, user, passwd);
					initOk = true;
				} catch (SQLException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		return initOk;
	}
	
	public static void saveLog(final XLoggerData data) throws Exception {
		storage[workerId.incrementAndGet() & MAX_CPU].save(data);
	}
	
}
