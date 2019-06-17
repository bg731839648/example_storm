package storm.lesson.partitionTransactional.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import storm.lesson.partitionTransactional.spout.MyPtTxSpout;
import storm.lesson.transactional_daily.bolt.MyDailyBatchBolt;
import storm.lesson.transactional_daily.bolt.MyDailyCommitterBolt;

public class MyPtTxTopolpgy {

    public static void main(String[] args) {
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("txId", "spoutId", new MyPtTxSpout(), 1);
        builder.setBolt("txBolt", new MyDailyBatchBolt(), 3).shuffleGrouping("spoutId");
        builder.setBolt("committerSum", new MyDailyCommitterBolt(), 1).shuffleGrouping("txBolt");

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
