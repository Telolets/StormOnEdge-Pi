import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.util.List;
import static backtype.storm.utils.Utils.tuple;

/**
 * Created by ken on 12/14/2015.
 */
public class SOERabbitMQScheme implements Scheme {
  public List<Object> deserialize(byte[] ser) {
    return tuple(new String(ser));
  }

  public Fields getOutputFields() {
    return new Fields("message");
  }
}
