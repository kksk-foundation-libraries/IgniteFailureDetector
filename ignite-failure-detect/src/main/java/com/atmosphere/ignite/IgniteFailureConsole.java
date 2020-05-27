package com.atmosphere.ignite;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

public class IgniteFailureConsole {

	public static void main(String[] args) {
		if (args == null || args.length < 3) {
			System.err.println("please set arguments[String zkConnString, String zkWatchPath, long normalNodes]");
			System.exit(9);
			return;
		}
		try {
			System.out.format("start register zkConnString:%s, zkWatchPath:%s, normalNodes:%s\n", args[0], args[1], args[2]);
			invoke(args[0], args[1], Long.parseLong(args[2]));
			System.out.format("end   register zkConnString:%s, zkWatchPath:%s, normalNodes:%s\n", args[0], args[1], args[2]);
			System.exit(0);
		} catch (NumberFormatException e) {
			System.err.println("please set arguments[String zkConnString, String zkWatchPath, long normalNodes]");
			System.err.println("illegal argument[long normalNodes]");
			System.exit(8);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(7);
		}
	}

	public static void invoke(String zkConnString, String zkWatchPath, long normalNodes) throws Exception {
		CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(100, 3));
		byte[] data = new byte[Long.BYTES];
		putLong(data, 0, normalNodes);
		createPath(client, zkWatchPath, data, true);
	}

	private static void createPath(CuratorFramework client, String path, byte[] data, boolean withData) throws Exception {
		if (client.checkExists().forPath(path) != null) {
			return;
		}
		ZKPaths.PathAndNode pan = ZKPaths.getPathAndNode(path);
		if (pan.getPath().length() > 1) {
			createPath(client, pan.getPath(), data, false);
		}
		if (!withData) {
			client.create().forPath(path);
		} else {
			client.create().forPath(path, data);
		}
	}

	private static void putLong(byte[] b, int off, long val) {
		b[off + 7] = (byte) (val);
		b[off + 6] = (byte) (val >>> 8);
		b[off + 5] = (byte) (val >>> 16);
		b[off + 4] = (byte) (val >>> 24);
		b[off + 3] = (byte) (val >>> 32);
		b[off + 2] = (byte) (val >>> 40);
		b[off + 1] = (byte) (val >>> 48);
		b[off] = (byte) (val >>> 56);
	}
}
