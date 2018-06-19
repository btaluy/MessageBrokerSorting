package Classes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class JobTracker {
  private static ExecutorService executor;
  private static AtomicInteger threadsInProgress;

  public JobTracker() {
    executor = Executors.newCachedThreadPool();
    // executor = Executors.newWorkStealingPool();
  }

  public void start(){
    threadsInProgress = new AtomicInteger(0);
  }

  public int spawn(Runnable r) {
    executor.submit(r);
    int running = threadsInProgress.incrementAndGet();
    // System.out.println("Spawned, now running "+ running);
    return running;
  }

  public int done(){
    int running = threadsInProgress.decrementAndGet();
    // System.out.println("Done, now running "+ running);
    return running;
  }

  public boolean running() {
    int running = threadsInProgress.get();

    return running>0;
  }

  public void await(){
    while (running())
    {
      // stay idle while waiting for threads to finish
    }
  }
}
