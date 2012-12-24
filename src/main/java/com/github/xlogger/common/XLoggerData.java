package com.github.xlogger.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class XLoggerData {
	
	public String getTabName() {
		return tabName;
	}

	public long getEventId() {
		return eventId;
	}

	public Object[] getArgs() {
		return args;
	}

	public XLoggerData(String tabName, long eventId, Object[] args) {
		super();
		this.tabName = tabName;
		this.eventId = eventId;
		this.args = args;
	}

	public XLoggerData(ChannelBuffer buffer) {
		short tablen = buffer.readShort();
		byte[] tabbytes = new byte[tablen];
		buffer.readBytes(tabbytes);
		this.tabName = new String(tabbytes);
		this.eventId = buffer.readLong();
		int objlen = buffer.readInt();
		this.args = new Object[objlen];
		byte[] objsbyte = new byte[buffer.readableBytes()];
		buffer.readBytes(objsbyte);
		ByteArrayInputStream bais = new ByteArrayInputStream(objsbyte);
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(bais);
			for (int i = 0; i < objlen; i++) {
				this.args[i] = ois.readObject();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private String tabName;
	
	private long eventId;
	
	private Object[] args;
	
	public ChannelBuffer toBuffer() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(baos);
			for (Object obj: args) {
				oos.writeObject(obj);
			}
			byte[] data = baos.toByteArray();
			byte[] tabbytes = tabName.getBytes();
			int bufferlen = data.length + 8 + tabbytes.length + 2 + 4;
			ChannelBuffer buffer = ChannelBuffers.buffer(bufferlen + 4);
			buffer.writeInt(bufferlen);
			buffer.writeShort(tabbytes.length);
			buffer.writeBytes(tabbytes);
			buffer.writeLong(eventId);
			buffer.writeInt(args.length);
			buffer.writeBytes(data);
			return buffer;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException e) {
				}
			}
			try {
				baos.close();
			} catch (IOException e) {
			}
		}
	}
	
	public String toSQL() {
		StringBuilder sb = new StringBuilder();
		sb.append("insert delayed `").append(tabName).append("` values(null, null");
		for (int i = 0; i < args.length; i++) {
			sb.append(",?");
		}
		sb.append(")");
		return sb.toString();
	}
}
