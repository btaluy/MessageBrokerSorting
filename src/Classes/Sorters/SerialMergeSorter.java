package Classes.Sorters;
import Classes.Transaction;

/** Code can be found here: http://www.cs.armstrong.edu/liang/intro9e/html/MergeSort.html? **/
public class SerialMergeSorter {
  /** The method for sorting the numbers */
  public static void serialMergeSort(Transaction[] list) {
    if (list.length > 1) {
      // Merge sort the first half
      Transaction[] firstHalf = new Transaction[list.length / 2];
      System.arraycopy(list, 0, firstHalf, 0, list.length / 2);
      serialMergeSort(firstHalf);

      // Merge sort the second half
      int secondHalfLength = list.length - list.length / 2;
      Transaction[] secondHalf = new Transaction[secondHalfLength];
      System.arraycopy(list, list.length / 2, secondHalf, 0, secondHalfLength);
      serialMergeSort(secondHalf);

      // Merge firstHalf with secondHalf into list
      merge(firstHalf, secondHalf, list);
    }
  }

  /** Merge two sorted lists */
  public static void merge(Transaction[] list1, Transaction[] list2, Transaction[] temp) {
    int current1 = 0; // Current index in list1
    int current2 = 0; // Current index in list2
    int current3 = 0; // Current index in temp

    while (current1 < list1.length && current2 < list2.length) {
      if (list1[current1].compareTo(list2[current2]) <= -1)
        temp[current3++] = list1[current1++];
      else
        temp[current3++] = list2[current2++];
    }

    while (current1 < list1.length)
      temp[current3++] = list1[current1++];

    while (current2 < list2.length)
      temp[current3++] = list2[current2++];
  }
}
