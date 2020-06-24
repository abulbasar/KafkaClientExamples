package com.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


@Slf4j
public class ZkClient implements AutoCloseable{
    private ZooKeeper zooKeeper;


    public ZooKeeper connect(String host) throws IOException,InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(host,5000, (WatchedEvent we) -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        return zooKeeper;
    }

    public boolean exists(String path){
        return getStat(path) != null;
    }
    public Stat getStat(String path){
        try {
            return zooKeeper.exists(path, true);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public void create(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void update(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, version);
    }
    public byte[] getData(String path, Stat stat) throws KeeperException, InterruptedException {
        return  zooKeeper.getData(path, false, stat);
    }

    @Override
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void deleteIfExists(String path) throws KeeperException, InterruptedException {
        Stat stat = getStat(path);
        if(stat != null) {
            ZKUtil.deleteRecursive(zooKeeper, path);
        }
    }
}
