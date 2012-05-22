package com.github.xlogger;

import com.github.xlogger.client.XLoggerClient;
import com.github.xlogger.common.XLoggerData;

public final class XLogger {
	
	private static XLoggerClient client = null;
	
	private static Thread clientThread = null;
	/**
	 * 设置服务器地址和端口，以及Timout
	 * @param host
	 * @param port
	 * @param timeout
	 */
	synchronized public static void initXLogger(String host, short port, int timeout) {
		if (client == null) {
			client = new XLoggerClient(host, port, timeout);
			client.connect();
			clientThread = new Thread(client);
			clientThread.start();
		}
	}
	
	public static void log(String tabName, Object ... args) {
		final XLoggerData data = new XLoggerData(tabName, System.currentTimeMillis(), args);
		client.sendLog(data);
	}
	
}
