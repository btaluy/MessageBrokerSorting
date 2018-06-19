import Classes.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.codehaus.jettison.json.JSONException;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.List;

public class MergeClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws JSONException {
    /*try {
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
      TextMessage message = createTextMessage(session, Transaction.generateList(10000));

      // Tell the producer to send the message
      producer.send(message);

      // Generating a certain randomness for the exercise
      Thread.sleep(new Random().nextInt(5000));

      // Wait for the returning message
      MessageConsumer consumer = session.createConsumer(destination);
      Message returnMessage = consumer.receive();

      if (returnMessage instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) returnMessage;
        String text = textMessage.getText();

        try {
          List<Transaction> transactions = parseReturnMessage(text);





          System.out.println(transactions.size());
          System.out.println("--------- RESULTS: ---------");
//          System.out.println("K = " + result.getAmountOfClusters());
//          System.out.println("Result = " + result.getResult());
//          System.out.println("Threads = " + result.getNumThreads());
//          System.out.println("Runtime (ms) = " + result.getRunTime());
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
    }*/
  }


  /**
   * Transforming the JSON body to object
   *
   * @param text JSON
   * @return GregorySeriesReturn object
   * @throws IOException Exception
   */
  private static List<Transaction> parseReturnMessage(String text) throws JSONException {
    return Transaction.fromJSON(text);
  }

  private static TextMessage createTextMessage(Session session, List<Transaction> transactions) throws JMSException, JsonProcessingException {
    String jsonString = MAPPER.writeValueAsString(transactions);
    return session.createTextMessage(jsonString);
  }
}
