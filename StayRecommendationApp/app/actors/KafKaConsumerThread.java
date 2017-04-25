package actors;

import akka.actor.ActorRef;
import kafka.KafkaClientRecommendationResponseConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by vinay on 4/24/17.
 */
public class KafKaConsumerThread implements Runnable{


    private final ActorRef actorRef;
    private final KafkaClientRecommendationResponseConsumer consumerClient;


    public KafKaConsumerThread(ActorRef actorRef, KafkaClientRecommendationResponseConsumer consumerClient) {
        this.actorRef = actorRef;
        this.consumerClient = consumerClient;
    }

    @Override
    public void run() {

        while (true){
            ConsumerRecords<String,String> consumerRecords = consumerClient.consumeMessage();
            actorRef.tell(new ConsumerActor.ConsumeRecordsFromKafka(consumerRecords), null);
        }

    }
}
