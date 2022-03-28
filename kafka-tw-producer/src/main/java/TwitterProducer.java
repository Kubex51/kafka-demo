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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public static void main(String[] args) throws InterruptedException {
        //get twitter data
        //setup blocking queues
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        
        //declare host
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("kafka", "java", "python", "linux", "bitcoin");
        hosebirdEndpoint.trackTerms(terms);
        
        //Insert keys (those provided are revoked)
        Authentication hosebirdAuth = new OAuth1(
                "s3vaERZbLQzyv5Y3wkMGKvy7B",
                "6lbFsfWPI2Ql1WiqxV4MnCrtR2MWdWUwdDe6slYSu9ktNErKec",
                "4882187681-JZVMAlMkHCXgZbrdTGS5m6F72syVTsIs9BaAKTq",
                "kQ0sMB72iec4Jhr0gLcw8apKBUdgVWpMffdZzKSBu2AaS");
        
        //Client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        Client hosebirdClient = builder.build();

        
        // producer variables
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "TwitterSearch_topic";

        //create Producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //additional properties for ensuring producer safety
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //compression setting
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        //create the producer
        KafkaProducer<String, String> TwitterProducer = new KafkaProducer<String, String>(producerProperties);

        //connect client
        hosebirdClient.connect();

        int PostsToDownload = 200;
        int PostDownloaded = 0;

        //create logger

        Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        //ShutdownHook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down application...");
            logger.info("Stopping client...");
            hosebirdClient.stop();
            logger.info("Closing producer...");
            TwitterProducer.flush();
            TwitterProducer.close();
            logger.info("Application closed");
        }));

        while(!hosebirdClient.isDone() && PostsToDownload > 0) {
            //create producer record
            String msg = null;
            try{
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch(InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (msg != null) {
                logger.info(msg);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                        topic,
                        Integer.toString(PostDownloaded),
                        msg
                );
                PostDownloaded += 1;
                PostsToDownload -= 1;
                logger.info("Posts to Download: " + Integer.toString(PostsToDownload));
                TwitterProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Successfully pulled" + "\n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp() + "\n");
                        } else {
                            logger.error("Error while producing" + e);
                        }
                    }
                });
            }
        }
    }
}
