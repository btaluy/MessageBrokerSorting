package Classes.Sorters;

import Classes.Transaction;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/** Code can be found here: http://www.cs.armstrong.edu/liang/intro9e/html/ParallelMergeSort.html? **/
public class ParallelMergeSorter {
  public static void parallelMergeSort(Transaction[] list, Integer threads) {
    RecursiveAction mainTask = new ParallelMergeSorter.SortTask(list);
    ForkJoinPool pool = new ForkJoinPool(threads);
    pool.invoke(mainTask);
  }

  private static class SortTask extends RecursiveAction {
    private final int THRESHOLD = 500;
    private Transaction[] list;

    SortTask(Transaction[] list) {
      this.list = list;
    }

    @Override
    protected void compute() {
      if (list.length > 1) {
        if (list.length < THRESHOLD) {
          SerialMergeSorter.serialMergeSort(list);
        } else {
          // Obtain the first half
          Transaction[] firstHalf = new Transaction[list.length / 2];
          System.arraycopy(list, 0, firstHalf, 0, list.length / 2);

          // Obtain the second half
          int secondHalfLength = list.length - list.length / 2;
          Transaction[] secondHalf = new Transaction[secondHalfLength];
          System.arraycopy(list, list.length / 2,
              secondHalf, 0, secondHalfLength);

          // Recursively sort the two halves
          invokeAll(new ParallelMergeSorter.SortTask(firstHalf), new ParallelMergeSorter.SortTask(secondHalf));

          // Merge firstHalf with secondHalf into list
          SerialMergeSorter.merge(firstHalf, secondHalf, list);
        }
      }
    }
  }
}
