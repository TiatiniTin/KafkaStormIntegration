package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;


public class KafkaStormSample {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(true);
        String kafkaConnString = "localhost:29092";
        String topic = "WarAndPeace";

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                .builder(kafkaConnString,topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .build();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka-spout", new KafkaSpout<>(kafkaSpoutConfig));
        topologyBuilder.setBolt("word-splitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        topologyBuilder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-splitter");

        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology("KafkaStormSample", config, topologyBuilder.createTopology());

            Thread.sleep(10000);

            //cluster.shutdown();
        }
    }
}
