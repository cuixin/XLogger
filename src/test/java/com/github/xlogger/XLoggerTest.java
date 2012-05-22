package com.github.xlogger;

import static org.junit.Assert.*;

import org.junit.Test;

public class XLoggerTest {

	@Test
	public void test() {
		XLogger.initXLogger("localhost", (short) 4000, 30000);

		for (int j = 0; j < 10000; j++) {
			for (int i = 0; i < 10000; i++)
				XLogger.log("Test", "1111", 2323);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			Thread.sleep(50000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
