package com.github.xlogger;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.xlogger.server.XLoggerServer;

public class XLoggerServerTest {

	@Test
	public void test() {
        XLoggerServer.start("localhost", (short) 4000);
	}

}
