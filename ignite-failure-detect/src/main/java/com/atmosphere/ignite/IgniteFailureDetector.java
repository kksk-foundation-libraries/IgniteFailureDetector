package com.atmosphere.ignite;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.ignite.Ignite;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;

public class IgniteFailureDetector {
	private final IgniteEx igniteEx;
	private final CuratorFramework client;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final ConcurrentLinkedQueue<Object> pausing = new ConcurrentLinkedQueue<>();

	private static final AtomicReference<Status> status = new AtomicReference<>(Status.UNKNOWN);
	private static final AtomicInteger normalNodes = new AtomicInteger();

	@SuppressWarnings("resource")
	public IgniteFailureDetector(Ignite ignite, String zkConnString, String zkWatchPath) throws Exception {
		igniteEx = (IgniteEx) ignite;
		client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(100, 3));
		client.start();
		TreeCache cache = new TreeCache(client, zkWatchPath);
		cache.start();
		byte[] init = client.getData().forPath(zkWatchPath);
		int initNodes = getInt(init, 0);
		normalNodes.set(initNodes);
		if (igniteEx.cluster().nodes().stream().filter(n -> !n.isClient()).count() < initNodes - 1) {
			set(Status.FAILED);
		} else {
			set(Status.RUNNING);
		}

		cache.getListenable().addListener((c, event) -> {
			byte[] data = event.getData().getData();
			int nodes = getInt(data, 0);
			normalNodes.set(nodes);
			if (igniteEx.cluster().nodes().stream().filter(n -> !n.isClient()).count() < nodes - 1) {
				set(Status.FAILED);
			} else {
				set(Status.RUNNING);
			}
		});

	}

	static int getInt(byte[] b, int off) {
		return ((b[off + 3] & 0xFF)) + ((b[off + 2] & 0xFF) << 8) + ((b[off + 1] & 0xFF) << 16) + ((b[off]) << 24);
	}

	private void set(Status newStatus) {
		WriteLock writeLock = lock.writeLock();
		try {
			writeLock.lock();
			status.set(newStatus);
			if (Status.RUNNING.equals(newStatus)) {
				while (pausing.size() > 0) {
					pausing.poll().notify();
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	public void check() {
		ReadLock readLock = lock.readLock();
		try {
			readLock.lock();
			while (Status.UNKNOWN.equals(status.get())) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					break;
				}
			}
			if (!Status.RUNNING.equals(status.get())) {
				Object o = new Object();
				pausing.offer(o);
				try {
					o.wait();
				} catch (InterruptedException ignore) {
				}
			}
		} finally {
			readLock.unlock();
		}
	}

	public void start() {
		igniteEx.events().localListen(e -> {
			if (e instanceof DiscoveryEvent) {
				DiscoveryEvent event = (DiscoveryEvent) e;
				int nodes = normalNodes.get();
				if (event.topologyNodes().stream().filter(n -> !n.isClient()).count() < nodes - 1) {
					set(Status.FAILED);
				} else {
					set(Status.RUNNING);
				}
			}
			return true;
		}, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_JOINED);
	}

	private static enum Status {
		UNKNOWN, RUNNING, FAILED
	}
}
