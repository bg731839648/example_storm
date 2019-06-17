package storm.lesson.transactional.spout.eimtter;


import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Values;
import storm.lesson.transactional.spout.mata.MyMata;

import java.math.BigInteger;
import java.util.Map;

public class MyEimtter implements ITransactionalSpout.Emitter<MyMata> {

    Map<Long, String> dbMap;

    public MyEimtter(Map<Long, String> dbMap) {
        this.dbMap = dbMap;
    }

    @Override
    public void emitBatch(TransactionAttempt tx, MyMata myMata, BatchOutputCollector collector) {
        long beginPoint = myMata.getBeginPoint();
        int num = myMata.getNum();

        for (long i = beginPoint; i < num + beginPoint; i++) {

            // 如果数据源 还有数据,则取出后继续发送
            if (dbMap.get(i) != null) {
                collector.emit(new Values(tx, dbMap.get(i)));
            }
        }
    }

    @Override
    public void cleanupBefore(BigInteger bigInteger) {

    }

    @Override
    public void close() {

    }
}
