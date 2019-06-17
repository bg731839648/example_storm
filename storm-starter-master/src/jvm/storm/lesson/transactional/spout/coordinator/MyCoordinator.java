package storm.lesson.transactional.spout.coordinator;


import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.utils.Utils;
import storm.lesson.transactional.spout.mata.MyMata;

import java.math.BigInteger;

public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMata> {

    private static int BATCH_NUM = 10;

    @Override
    public MyMata initializeTransaction(BigInteger bigInteger, MyMata prevMetaData) {

        long beginPoint;

        if (prevMetaData == null) {
            beginPoint = 0L;
        } else {
            beginPoint = prevMetaData.getBeginPoint() + prevMetaData.getNum();
        }

        MyMata mata = new MyMata();
        mata.setBeginPoint(beginPoint);
        mata.setNum(BATCH_NUM);
//        System.err.println("mata: " + JSON.toJSONString(mata));
        return mata;
    }

    @Override
    public boolean isReady() {
        Utils.sleep(20);
        return true;
    }

    @Override
    public void close() {

    }
}
