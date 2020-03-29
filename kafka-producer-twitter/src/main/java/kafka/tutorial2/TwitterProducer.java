package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
  String consumerKey = "";
  String consumerSecret = "";
  String token = "";
  String secret = "";
  List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

  public TwitterProducer() {
  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    Client client = createTwitterClient(msgQueue);
    client.connect();

    // loop to send tweets to kafka
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
//        producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (Callback) (recordMetadata, e) -> {
//          if (e != null) {
//            logger.error("Something bad happened", e);
//          }
//        });
      }
    }
  }

  public Client createTwitterClient(BlockingQueue<String> queue) {
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
      .name("Hosebird-Client-01")                              // optional: mainly for the logs
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(queue));

    return builder.build();
  }
}
