package com.example.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.UUID;

public class ZkApp {

    public static void main(String... args) throws IOException, InterruptedException, KeeperException {

        ZkClient zkClient = new ZkClient();
        zkClient.connect("localhost:2181");
        final byte[] data = UUID.randomUUID().toString().getBytes();

        final String rootPath = "/example";

        if(!zkClient.exists(rootPath)){
            zkClient.create(rootPath, new byte[0]);
        }
        final String path = rootPath + "/version";

        long startTime = System.currentTimeMillis();
        long count = 10000;
        Stat stat = zkClient.getStat(path);
        if(stat == null) {
            zkClient.create(path, new byte[0]);
        }
        for (int i = 0; i < count; i++) {
            stat = zkClient.getStat(path);
            zkClient.update(path, data, stat.getVersion());
        }
        long duration = System.currentTimeMillis() - startTime;
        long eps = duration * 1000 / count;
        System.out.println(String.format("Write eps: %d", eps));
        startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            zkClient.getData(path, stat);
        }
        duration = System.currentTimeMillis() - startTime;
        eps = duration * 1000 / count;
        System.out.println(String.format("Read eps: %d", eps));

        zkClient.deleteIfExists(rootPath);

        System.exit(0);
    }
}
