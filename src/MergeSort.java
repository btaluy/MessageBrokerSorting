import Classes.EventProfiler;
import Classes.Sorters.ParallelMergeSorter;
import Classes.Sorters.SerialMergeSorter;
import Classes.Transaction;
import Classes.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.codehaus.jettison.json.JSONException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.List;
import java.util.Random;


public class MergeSort {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws JSONException {
    try {
      // Create a ConnectionFactory
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

      // Create a Connection
      Connection connection = connectionFactory.createConnection();
      connection.start();

      // Create a Session
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Create the destination (Topic or Queue)
      Destination destination = session.createQueue("Transactions");

      // Create a MessageProducer from the Session to the Topic or Queue
      MessageProducer producer = session.createProducer(destination);

      // Creating a message
      TextMessage message = createTextMessage(session, Transaction.generateList(1000000));
      System.out.println("Message created!");

      // Tell the producer to send the message
      producer.send(message);
      System.out.println("Sending message to queue!");

      // Generating a certain randomness for the exercise
      Thread.sleep(new Random().nextInt(5000));

      // Wait for the returning message
      MessageConsumer consumer = session.createConsumer(destination);
      Message returnMessage = consumer.receive();
      System.out.println("Message received!");

      if (returnMessage instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) returnMessage;
        String text = textMessage.getText();

        try {
          List<Transaction> transactions = parseReturnMessage(text);

          System.out.println("--------- RESULTS: ---------");
          EventProfiler profiler = new EventProfiler(true);
          benchmark(transactions, profiler);

        } catch(JSONException e) {
          throw new JSONException(e);
        }
      } else {
        throw new IllegalArgumentException("Unexpected message " + message);
      }

      session.close();
      connection.close();
    } catch (JMSException | IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void benchmark(List<Transaction> transactions, EventProfiler profiler) {
    profiler.start();
    generateText(profiler, transactions.size());

    System.out.println("-----------------------------------------------------------------");
    serialBenchmark(transactions, profiler);
    // Uses two threads for the parallel mergesort.
    parallelBenchmark(transactions, 2, profiler);

    // Uses four threads for the parallel mergesort.
    parallelBenchmark(transactions, 4, profiler);

    // Uses six threads for the parallel mergesort.
    parallelBenchmark(transactions, 6, profiler);

    // Uses eight threads for the parallel mergesort.
    parallelBenchmark(transactions, 8, profiler);

    System.out.println("-----------------------------------------------------------------\n\n");
  }


  private static void serialBenchmark(List<Transaction> transactions, EventProfiler profiler) {
    /** Serial merge sorting. **/
    profiler.start();

    Transaction[] TransactionArray = transactions.toArray(new Transaction[transactions.size()]);
    SerialMergeSorter.serialMergeSort(TransactionArray);

    if (Utils.isFilledArray(TransactionArray)) {
      profiler.log("Serial MergeSort Done");
    } else {
      profiler.log("Serial MergeSort failed");
    }
  }

  private static void parallelBenchmark(List<Transaction> transactions, Integer threadsAmount, EventProfiler profiler) {
    /** Parallel merge sorting **/
    Transaction[] arrayForOwnParallelSort = transactions.toArray(new Transaction[transactions.size()]);

    profiler.start();
    ParallelMergeSorter.parallelMergeSort(arrayForOwnParallelSort, threadsAmount);

    if (Utils.isFilledArray(arrayForOwnParallelSort)) {
      profiler.log("Parallel MergeSort Done while using " + threadsAmount + " threads");
    } else {
      profiler.log("Parallel MergeSort failed");
    }
  }

  private static void generateText(EventProfiler profiler, Integer size) {
    System.out.println("////////////////////////////\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");
    profiler.log("Filling array with " + size + " transactions");
    System.out.println("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\///////////////////////////");
    System.out.println();
  }

  private static List<Transaction> parseReturnMessage(String text) throws JSONException {
    return Transaction.fromJSON(text);
  }

  private static TextMessage createTextMessage(Session session, List<Transaction> transactions) throws JMSException, JsonProcessingException {
    String jsonString = MAPPER.writeValueAsString(transactions);
    return session.createTextMessage(jsonString);
  }
}

