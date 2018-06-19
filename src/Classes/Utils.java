package Classes;

public class Utils {


  public static void swap(int[] ar, int i, int j) {
    int temp = ar[i];
    ar[i] = ar[j];
    ar[j] = temp;
  }

  public static int[] fillArray(int amount) {
    int[] result = new int[amount];
    for (int i=0; i<amount; i++){
      result[i] = i;
    }
    return result;
  }

  public static boolean isFilledArray(Transaction[] transactions)
  {
    return transactions.length > 0;
  }
}
