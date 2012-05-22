package com.github.xlogger.server.store;

import com.github.xlogger.common.XLoggerData;

public interface Storage {
	public void save(XLoggerData data) throws Exception;
}
