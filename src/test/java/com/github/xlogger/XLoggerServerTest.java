package com.github.xlogger;

import org.junit.Test;

import com.github.xlogger.server.XLoggerServer;
import com.github.xlogger.server.store.StorageFactory;

public class XLoggerServerTest {

	@Test
	public void test() {
		StorageFactory.setupJDBC("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.10.73/test?", "root", "123456");
        XLoggerServer.start("localhost", (short) 4000);
        
        while (true) {
			try {
				Thread.sleep(1000000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
