package com.github.xlogger.server.codec;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import com.github.xlogger.common.XLoggerData;

/**
 * 解析发送过来的消息
 * @author Steven
 *
 */
public class XLoggerDecoder extends LengthFieldBasedFrameDecoder {
	public XLoggerDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
		super(maxFrameLength, lengthFieldOffset, lengthFieldLength, 0, 4);
	}

	public Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
		ChannelBuffer objBuffer = (ChannelBuffer) super.decode(ctx, channel, buffer);
		if (objBuffer != null) {
			XLoggerData data = new XLoggerData(objBuffer);
			return data;
		} 
		return null;
    }
}
