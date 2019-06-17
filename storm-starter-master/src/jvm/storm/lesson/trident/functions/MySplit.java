package storm.lesson.trident.functions;


import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class MySplit extends BaseFunction {

    private static final long serialVersionUID = -5984643709941609732L;

    String patton;

    public MySplit(String patton) {
        this.patton = patton;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String log = tuple.getString(0);
        String[] split = log.split(patton);

        if (split.length >= 3) {
            collector.emit(new Values(split[2], "cf", "pvCount", split[1]));
        }
    }
}
