package philosophersProblem;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class Philisopher implements Watcher {
    private int id;
    private String hostPort;
    private String root;
    private Semaphore left;
    private Semaphore right;
    private int seats;
    private Object mutex;
    private ZooKeeper zk;
    private String node;

    public Philisopher(int id, String hostPort, String root, Semaphore left, Semaphore right, int seats) throws Exception {
        this.id = id;
        this.node = root.concat("/").concat(Integer.toString(id));
        this.zk = new ZooKeeper(hostPort, 3000, this);
        if (this.zk == null) {
            throw new Exception("zooKeeper is not initialized");
        }
        this.root = root;
        this.mutex = new Object();
        this.left = left;
        this.right = right;
        this.seats = seats;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public void performEating() throws InterruptedException, KeeperException {
        System.out.println(String.format("Philosopher %s is going to eat", id));
        synchronized (mutex) {
            boolean created = false;
            while (true) {
                if (!created) {
                    zk.create(node, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    created = true;
                }
                List<String> active = zk.getChildren(root, this);
                if (active.size() > seats) {
                    zk.delete(node, -1);
                    mutex.wait(3000);
                    Thread.sleep((new Random()).nextInt(5)* 100);
                    created = false;
                } else {
                    left.acquire();
                    System.out.println(String.format("Philosopher %s picked up the left fork", id + 1));
                    right.acquire();
                    System.out.println(String.format("Philosopher %s picked up the right fork", id + 1));

                    Thread.sleep(((new Random()).nextInt(5) + 1) * 1000);

                    right.release();
                    System.out.println(String.format("Philosopher %s put the right fork", id + 1));
                    left.release();
                    System.out.println(String.format("Philosopher %s put the loft fork and finished eating", id + 1));
                    break;
                }
            }
        }
    }

    public void performThinking() throws InterruptedException, KeeperException {
        System.out.println(String.format("Philosopher %s is thinking", id));
        zk.delete(node, -1);
        Thread.sleep(((new Random()).nextInt(5) + 1) * 1000);
    }
}
