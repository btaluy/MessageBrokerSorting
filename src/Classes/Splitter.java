package Classes;

public class Splitter implements Runnable {

  private int[] anArray;
  private int low;
  private int high;
  public int pivotIndex;


  public Splitter(int[] anArray) {
    this.anArray = anArray;
    this.low = 0;
    this.high = anArray.length-1;
  }

  public Splitter(int[] anArray, int low, int high, int pivot) {
    this.anArray = anArray;
    this.low = low;
    this.high = high;
  }

  public String toString() {
    return ""+pivotIndex;
  }

  public void pivotSplit(int pivot)
  {
//        System.out.println("Splitting on "+pivot);
    this.pivotIndex = pivotPartition(anArray, low, high, pivot);
  }

  public int pivotPartition(int[] list, int first, int last, int pivot) {

    int i = first; // Index for forward search
    int j = last ; // Index for backward search

    while (i < j) {
      // Search forward from left
      while (i <= j && list[i] <= pivot)
        i++;

      // Search backward from right
      while (i <= j && list[j] > pivot)
        j--;

      // Swap two elements in the list
      if (i < j) {
        Utils.swap(list, i, j);
//                System.out.print("["+i + "," + j+"]");
//                Utils.printArray(list);
      }
    }

//        System.out.print("["+i + "," + j+"]");
    // list [low..j] contains elements <= pivot
    // list [j+1..high] contains element >= pivot
    return j;
  }

  @Override
  public void run() {
  }
}
