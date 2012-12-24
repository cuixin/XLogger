package com.github.xlogger.server;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.github.xlogger.common.XLoggerData;
import com.github.xlogger.server.codec.XLoggerDecoder;
import com.github.xlogger.server.store.StorageFactory;

public class XLoggerServer {
    private static final Logger logger = Logger.getLogger(LoggerHandler.class.getName());

    private static final ConcurrentMap<Channel, Long> connectionMap = new ConcurrentHashMap<Channel, Long>();
    
    private static final ConcurrentLinkedQueue<XLoggerData> unWroteQueue = new ConcurrentLinkedQueue<XLoggerData>();
    
	private static class LoggerHandler extends SimpleChannelUpstreamHandler {
		AtomicInteger count = new AtomicInteger();
		
		@Override
	    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			connectionMap.put(ctx.getChannel(), System.currentTimeMillis());
	    }
	    
		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
			connectionMap.remove(ctx.getChannel());
		}
		
	    @Override
	    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
//	        e.getChannel().write(e.getMessage());
	    	if (e.getMessage() instanceof XLoggerData) {
	    		final XLoggerData data = (XLoggerData)e.getMessage();
	    		try {
					StorageFactory.saveLog(data);
				} catch (Exception e1) {
					e1.printStackTrace();
					unWroteQueue.add(data);
					if (unWroteQueue.size() > 100) {
						logger.warning("too many messages not wrote!!!");
					}
				}
	    		if (count.incrementAndGet() % 10000 == 0)
	    			logger.info(data.getTabName() + " time = " + data.getEventId());
	    	}
	    }
	
	    @Override
	    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
	        // Close the connection when an exception is raised.
	        logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
	        e.getChannel().close();
	    }
	}
	
	private static LoggerHandler handler = new LoggerHandler();

	public static void start(String host, short port) {
		ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		bootstrap.setOption("tcpNoDelay", true);
		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new XLoggerDecoder(1024 * 10, 0, 4), handler);
			}
		});
		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(host, port));
		Timer idleTimer = new Timer("IdleTimer", true);
		idleTimer.scheduleAtFixedRate(new TimerTask() {
			
			@Override
			public void run() {
				for (Channel c: connectionMap.keySet()) {
					if (c.isConnected()) {
						try {
							ChannelBuffer cb = ChannelBuffers.buffer(1);
							cb.writeByte(100);
							c.write(cb);
						} catch (Exception e) {
							logger.info(e.getCause().getMessage());
						}
					}
				}
			}
		}, 1000, 10000);
	}
	
	public static void startInThread(final String host, final short port) {
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				start(host, port);
			}
		}, "XLoggerServer");
		t.setDaemon(true);
		t.start();
	}
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("You have to configurate the jdbcDriver, url, user, passwd");
			return;
		} else {
			if (StorageFactory.setupJDBC(args[0], args[1], args[2], args[3])) {
				System.out.println("Connect the database is ok");
				start("localhost", (short)4000);
			}
		}
	}
}
