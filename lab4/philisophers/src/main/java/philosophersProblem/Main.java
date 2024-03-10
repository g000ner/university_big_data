package philosophersProblem;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String hostPort = "localhost:2181";
        int philosophersCount = 5;
        int seats = philosophersCount - 1;

        Semaphore[] forks = new Semaphore[philosophersCount];
        for (int j = 0;j < philosophersCount; j++) {
            forks[j] = new Semaphore(1);
        }

        Thread[] threads = new Thread[philosophersCount];
        for (int id = 0; id < philosophersCount; id++) {
            int tId = id;
            System.out.println(tId);
            threads[id] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        int i = (tId + 1) % philosophersCount;
                        Philisopher philisopher = new Philisopher(tId, hostPort, "/philosophers", forks[tId], forks[i], seats);
                        for (int j = 0; j < 2; j++) {
                            philisopher.performEating();
                            philisopher.performThinking();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            System.out.println("aaaa");
            threads[id].setDaemon(false);
            threads[id].run();
        }

        for (int id = 0; id < philosophersCount; id++) {
            threads[id].join();
        }
    }
}
