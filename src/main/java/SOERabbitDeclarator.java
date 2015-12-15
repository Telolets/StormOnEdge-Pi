import com.rabbitmq.client.Channel;
import io.latent.storm.rabbitmq.Declarator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ken on 12/15/2015.
 */
public class SOERabbitDeclarator  implements Declarator {
  private final String exchange;
  private final String queue;
  private final String routingKey;

  public SOERabbitDeclarator(String exchange, String queue) {
    this(exchange, queue, "");
  }

  public SOERabbitDeclarator(String exchange, String queue, String routingKey) {
    this.exchange = exchange;
    this.queue = queue;
    this.routingKey = routingKey;
  }

  public void execute(Channel channel) {
    // you're given a RabbitMQ Channel so you're free to wire up your exchange/queue bindings as you see fit
    try {
      Map<String, Object> args = new HashMap<String, Object>();
      channel.queueDeclare(queue, false, false, false, args);
      //channel.exchangeDeclare(exchange, "topic", true);
      //channel.queueBind(queue, exchange, routingKey);
    } catch (IOException e) {
      throw new RuntimeException("Error executing rabbitmq declarations.", e);
    }
  }
}