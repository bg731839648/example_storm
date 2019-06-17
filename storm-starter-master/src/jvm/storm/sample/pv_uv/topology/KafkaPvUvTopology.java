package storm.sample.pv_uv.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.common.constant.KafkaConstant;
import storm.sample.pv_uv.bolt.KafkaPvUvPartBolt;
import storm.sample.pv_uv.bolt.PvUvPartBolt;
import storm.sample.pv_uv.bolt.PvUvWholeBolt;

/**
 * Storm Kafka Topology
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
public class KafkaPvUvTopology {

    /**
     * 主函数
     */
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig.Builder<String, String> kafkaBuilder = KafkaSpoutConfig.builder(KafkaConstant.TEST_ADDRESS, KafkaConstant.TOPICS_20190613);
        // 设置kafka属于哪个组
        kafkaBuilder.setProp("group.id", KafkaConstant.TOPICS_20190613);
        // 创建kafkaSpoutConfig
        KafkaSpoutConfig<String, String> build = kafkaBuilder.build();
        // 通过kafkaSpoutConfig获得kafkaSpout
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(build);
        builder.setSpout("spout", kafkaSpout);
        // 接受Kafka数据
        builder.setBolt("kafkaBolt", new KafkaPvUvPartBolt(), 8).shuffleGrouping("spout");
        // sessionTd分组
        builder.setBolt("bolt", new PvUvPartBolt(), 8).fieldsGrouping("kafkaBolt", new Fields("log"));
        // pv uv汇总
        builder.setBolt("sumBolt", new PvUvWholeBolt(), 1).shuffleGrouping("bolt");

        // 启动topology的配置信息-定义集群分配多少个工作进程来执行这个topology
        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(1);

        try {
            //  本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("kafkaStormTopology", config, builder.createTopology());

            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}