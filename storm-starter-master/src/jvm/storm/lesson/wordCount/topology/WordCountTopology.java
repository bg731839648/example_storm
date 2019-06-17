package storm.lesson.wordCount.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.lesson.wordCount.bolt.MyCountBolt;
import storm.lesson.wordCount.bolt.MySplitBolt;
import storm.lesson.wordCount.bolt.MySumBolt;
import storm.lesson.wordCount.spout.MyRandomSentenceSpout;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MyRandomSentenceSpout(), 1);

        // 分割字符串
        builder.setBolt("split", new MySplitBolt(" "), 8).shuffleGrouping("spout");
        // 统计字符串
        builder.setBolt("count", new MyCountBolt(), 3).fieldsGrouping("split", new Fields("word"));
        // 聚合求和
        builder.setBolt("sum", new MySumBolt(), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
