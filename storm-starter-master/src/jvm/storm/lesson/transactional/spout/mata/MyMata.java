package storm.lesson.transactional.spout.mata;

import java.io.Serializable;

public class MyMata implements Serializable {

    private static final long serialVersionUID = -1406849882414211126L;

    //事务开始位置
    private Long beginPoint = 0L;

    //batch的tuple数量
    private  int num;

    public Long getBeginPoint() {
        return beginPoint;
    }

    public void setBeginPoint(Long beginPoint) {
        this.beginPoint = beginPoint;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "啓動一個事務 MyData{" +
                "beginPoint=" + beginPoint +
                ", num=" + num +
                '}';
    }
}
