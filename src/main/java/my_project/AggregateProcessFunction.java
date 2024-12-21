package my_project;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashSet;

public class AggregateProcessFunction extends KeyedProcessFunction<String, InnerMessage, String> {
    private ValueState<HashSet<InnerMessage>> alive;
    private ValueState<Double> volume;

    public AggregateProcessFunction() {
        super();
    }

    @Override
    public void processElement(InnerMessage innerMessage, Context context, Collector<String> collector) throws Exception {
        initState();
        switch (innerMessage.type) {
            case AGGREGATE: {
                HashSet<InnerMessage> aliveSet = alive.value();
                aliveSet.add(innerMessage);
                alive.update(aliveSet);
                //        sum(l_extendedprice * (1 - l_discount))
                Double volumeValue = volume.value();
                volumeValue += (Double.parseDouble(innerMessage.getValueByKey("l_extendedprice")) * (1 - Double.parseDouble(innerMessage.getValueByKey("l_discount"))));
                volume.update(volumeValue);
                break;
            }
            case AGGREGATE_DELETE:{
                HashSet<InnerMessage> aliveSet = alive.value();
                aliveSet.remove(innerMessage);
                this.alive.update(aliveSet);
                //        sum(l_extendedprice * (1 - l_discount))
                Double volumeValue = volume.value();
                volumeValue -= (Double.parseDouble(innerMessage.getValueByKey("l_extendedprice")) * (1 - Double.parseDouble(innerMessage.getValueByKey("l_discount"))));
                volume.update(volumeValue);
                break;
            }
            default:
                throw new Exception("Invalid operation type: " + innerMessage.type);
        }

        String s = "";
        s += '|';
        s += innerMessage.getValueByKey("n_name").toString() + '|';
        s += innerMessage.getValueByKey("n2_name").toString() + '|';
        s += innerMessage.getValueByKey("l_shipdate").toString() + '|';
        // keep .4 precision, use abs to avoid negative zero (-0.0000 is considered as 0.0000, volume is a positive number)
        s += String.format("%.4f", Math.abs(this.volume.value())) + '|';
        collector.collect(s);



    }

    // To avoid null pointer exceptions, initialize the state for the state that used it in an increment way
    private void initState() throws IOException {
        if (alive.value() == null) {
            alive.update(new HashSet<>());
        }
        if (volume.value() == null) {
            volume.update(0.0);
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);


        alive = getRuntimeContext().
                getState(new ValueStateDescriptor<>(
                        "AggregateProcessFunction" + "alive",
                        TypeInformation.of(new TypeHint<HashSet<InnerMessage>>() {
                        })
                ));

        volume = getRuntimeContext().
                getState(new ValueStateDescriptor<>(
                        "AggregateProcessFunction" + "volume",
                        TypeInformation.of(Double.class)
                ));
    }
}
