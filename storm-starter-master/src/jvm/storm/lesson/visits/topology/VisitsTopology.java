package storm.lesson.visits.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import storm.lesson.local.spout.LocalSpout;
import storm.lesson.visits.bolt.PartPvBolt;
import storm.lesson.visits.bolt.WholePvBolt;

public class VisitsTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new LocalSpout(), 1);
        builder.setBolt("bolt", new PartPvBolt(), 32).shuffleGrouping("spout");
        builder.setBolt("sumBolt",new WholePvBolt(),1).shuffleGrouping("bolt");

        Config config = new Config();
        config.put(Config.TOPOLOGY_WORKERS, 1);

        try {
            if (args != null && args.length > 0) {
                // 集群提交模式
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } else {
                // 本地提交模式
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology("topology", config, builder.createTopology());

                Thread.sleep(10000);
                localCluster.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
