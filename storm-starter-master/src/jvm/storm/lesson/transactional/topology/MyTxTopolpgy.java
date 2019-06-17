package storm.lesson.transactional.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import storm.lesson.transactional.bolt.TestTxBolt;
import storm.lesson.transactional.bolt.TestTxCommitterBolt;
import storm.lesson.transactional.spout.MyTxSpout;

public class MyTxTopolpgy {

    public static void main(String[] args) {
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("txId", "spoutId", new MyTxSpout(), 1);
        builder.setBolt("txBolt", new TestTxBolt(), 1).shuffleGrouping("spoutId");
        builder.setBolt("committerSum", new TestTxCommitterBolt(), 1).shuffleGrouping("txBolt");


//        builder.setBolt("txBolt", new MyTxBolt(), 3).shuffleGrouping("spoutId");
//        builder.setBolt("committerSum", new MyTxCommitterBolt(), 1).shuffleGrouping("txBolt");

        Config config = new Config();
        // 打印emit 发射数据
        config.setDebug(false);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.buildTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("myTopology", config, builder.buildTopology());
        }
    }

}
