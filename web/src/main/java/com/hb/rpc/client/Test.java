package com.hb.rpc.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.taobao.zeus.socket.SocketLog;
import com.taobao.zeus.socket.worker.reqresp.WorkerHeartBeat;
import com.taobao.zeus.util.DateUtil;
import com.taobao.zeus.util.ZeusStringUtil;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ScheduledExecutorService service = Executors.newScheduledThreadPool(2);
		service.scheduleAtFixedRate(new Runnable() {
			private WorkerHeartBeat heartbeat = new WorkerHeartBeat();
			private int failCount = 0;

			@Override
			public void run() {
				System.out.println("failCount");
			}
		}, 0, 5, TimeUnit.SECONDS);
	}

}
