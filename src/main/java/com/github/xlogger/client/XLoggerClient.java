package com.github.xlogger.client;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.github.xlogger.common.XLoggerData;

public class XLoggerClient implements Runnable {
	
	private static final Logger logger = Logger.getLogger(XLoggerClient.class.getName());
	
	private Queue<XLoggerData> loggerQueue = new ConcurrentLinkedQueue<XLoggerData>();

	private class XLoggerHandler extends SimpleChannelUpstreamHandler {
		
		private Channel channel = null;
		
		public void flushData() {
			if (connected && channel != null) {
				XLoggerData data = null;
				while ((data = loggerQueue.poll()) != null) {
					if (channel.isOpen())
						channel.write(data.toBuffer());
					else {
						connected = false;
						loggerQueue.add(data);
						connect();
						break;
					}
				}
			}
		}
		
		@Override
		public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
			this.channel = ctx.getChannel();
			connected = true;
			lastIoTime = System.currentTimeMillis();
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			lastIoTime = System.currentTimeMillis();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			logger.log(Level.WARNING, 
					"Unexpected exception from downstream.", e.getCause().getMessage());
			if (channel != null)
				e.getChannel().close();
			connected = false;
		}
	}
	
	private XLoggerHandler handler = new XLoggerHandler();
	
	private volatile boolean connected;
	
	private boolean isConnected() {
		return connected;
	}
	
	private String host;
	
	private short port;
	
	private int timeout;
	
	private ClientBootstrap bootstrap;
	
	private volatile boolean isRunning = false;
	
	public XLoggerClient(String host, short port, int timeout) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		bootstrap = new ClientBootstrap(
                		new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        // Set up the pipeline factory.
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(handler);
			}
		});
		
		bootstrap.setOption("tcpNoDelay", "true");
		bootstrap.setOption("keepAlive", true);
		isRunning = true;
	}
	
	public boolean connect() {
        // Start the connection attempt.
		try {
			ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
			future.awaitUninterruptibly();
			logger.info("success = " + future.isSuccess());
	        future.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						connected = true;
						logger.info("connected ok");
					}
				}
			});
	        return true;
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		return false;
	}

	private void onShutdown() {
		connected = false;
		bootstrap.releaseExternalResources();
	}
	
	public void sendLog(final XLoggerData data) {
		synchronized (loggerQueue) {
			loggerQueue.add(data);
			loggerQueue.notifyAll();
		}
	}

	private long lastIoTime;
	
	/**
	 * 检测连接是否超时和定时发送ping消息
	 */
	@Override
	public void run() {
		while (isRunning) {
			if (!isConnected()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
			long now = System.currentTimeMillis();
			if (now - lastIoTime >= timeout) {
				// io 超时
				// 连接远程服务器超时,需要重连
				connect();
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// ignore
				}
			}
			synchronized (loggerQueue) {
				if (loggerQueue.isEmpty()) {
					try {
						loggerQueue.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			handler.flushData();
		}
		onShutdown();
	}

}
