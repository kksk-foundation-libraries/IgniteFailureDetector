package com.atmosphere.ignite;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

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
	private final LinkedList<Object> pausing = new LinkedList<>();
	private final long timeout;

	private static final AtomicLong normalNodes = new AtomicLong();
	private static final AtomicLong currentNodes = new AtomicLong();

	@SuppressWarnings("resource")
	public IgniteFailureDetector(Ignite ignite, long timeout, String zkConnString, String zkWatchPath) throws Exception {
		igniteEx = (IgniteEx) ignite;
		this.timeout = timeout;
		client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(100, 3));
		client.start();
		TreeCache cache = new TreeCache(client, zkWatchPath);
		cache.start();
		setNormalNodes(client.getData().forPath(zkWatchPath));
		reflesh();

		cache.getListenable().addListener((c, event) -> {
			setNormalNodes(event.getData().getData());
			reflesh();
		});
	}

	private void setNormalNodes(byte[] data) {
		long nodes = getLong(data, 0);
		normalNodes.set(nodes);
	}

	private long getLong(byte[] b, int off) {
		return ((b[off + 7] & 0xFFL)) + //
				((b[off + 6] & 0xFFL) << 8) + //
				((b[off + 5] & 0xFFL) << 16) + //
				((b[off + 4] & 0xFFL) << 24) + //
				((b[off + 3] & 0xFFL) << 32) + //
				((b[off + 2] & 0xFFL) << 40) + //
				((b[off + 1] & 0xFFL) << 48) + //
				(((long) b[off]) << 56);
	}

	private void reflesh() {
		if (currentNodes.get() >= normalNodes.get() - 1L) {
			synchronized (pausing) {
				for (Iterator<Object> ite = pausing.iterator(); ite.hasNext();) {
					Object o = ite.next();
					o.notify();
					ite.remove();
				}
			}
		}
	}

	public void check() throws InterruptedException {
		if (currentNodes.get() < normalNodes.get() - 1L) {
			Object o = new Object();
			synchronized (pausing) {
				pausing.add(o);
			}
			o.wait(timeout);
		}
	}

	public void start() {
		igniteEx.events().localListen(e -> {
			if (e instanceof DiscoveryEvent) {
				DiscoveryEvent event = (DiscoveryEvent) e;
				currentNodes.set(event.topologyNodes().stream().filter(n -> !n.isClient()).count());
				reflesh();
			}
			return true;
		}, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_JOINED);
	}
}
