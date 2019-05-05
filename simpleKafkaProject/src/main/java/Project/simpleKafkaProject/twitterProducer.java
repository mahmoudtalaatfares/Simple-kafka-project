package Project.simpleKafkaProject;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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



public class twitterProducer {
	
	
	
	 Logger logger = LoggerFactory.getLogger(twitterProducer.class.getName());

	    // twitter credentials 
	    String consumerKey = "VfkufTxDKjN9D3PrZSvrd2w62";
	    String consumerSecret = "eJKfrFbGupFKjfXL1kWI7GBf5mPr7WfUVoa7WRLvqICY9EhhP3";
	    String token = "874969095502008321-hJgzaXfK8c5lQXuaAT74WQDaOFEC1Mf";
	    String secret = "f7ud5AjY8ImszWMxbJyw4z0YpdUgoWI9fZ6gXwkdtvhne";
	    
        //tweets include these words 
	    List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

	    public twitterProducer(){}

	    public static void main(String[] args) {
	        new twitterProducer().run();
	    }

	    public void run(){

	        logger.info("Setup");
	        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

	        // create a twitter client
	        Client client = createTwitterClient(msgQueue);
	        // Attempts to establish a connection.
	        client.connect();
	        // create a kafka producer
	        KafkaProducer<String, String> producer = createKafkaProducer();
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
	            if (msg != null){
	                logger.info(msg);
	                //producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
	                  
	               // }); 
	                
	                ProducerRecord<String, String> record =
	                        new ProducerRecord<String, String>("tweets_topic", null, msg);
	                producer.send(record);
	                                
	            }
	        }
	        logger.info("End of application");
	    }

	    public Client createTwitterClient(BlockingQueue<String> msgQueue){
	        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
	        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
	        hosebirdEndpoint.trackTerms(terms);
	        
	        // These secrets should be read from a config file
	        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

	        ClientBuilder builder = new ClientBuilder()
	                .name("Hosebird-Client-01")                              
	                .hosts(hosebirdHosts)
	                .authentication(hosebirdAuth)
	                .endpoint(hosebirdEndpoint)
	                .processor(new StringDelimitedProcessor(msgQueue));

	        Client hosebirdClient = builder.build();
	        return hosebirdClient;
	    }

	    public KafkaProducer<String, String> createKafkaProducer(){
	        String bootstrapServers = "127.0.0.1:9092";

	        // create Producer properties
	        Properties properties = new Properties();
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

	       

	        // create the producer
	        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
	        return producer;
	    }

}
