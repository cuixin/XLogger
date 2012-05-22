package com.github.xlogger.server.store;

import java.sql.SQLException;

import com.github.xlogger.common.XLoggerData;
import com.github.xlogger.server.store.imp.JDBCStorage;

public class StorageFactory {

	private static boolean isInitialized = false;
	
	private static Storage storage;
	
	synchronized public static void setupJDBC(String jdbcDriver, String url, String user, String passwd) {
		if (!isInitialized) {
			try {
				storage = new JDBCStorage(jdbcDriver, url, user, passwd);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void saveLog(XLoggerData data) throws Exception {
		storage.save(data);
	}
	
}
