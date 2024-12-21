package my_project;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashSet;

import static my_project.InnerMessage.OperationType.AGGREGATE;
import static my_project.InnerMessage.OperationType.AGGREGATE_DELETE;

public class MixedProcessFunction extends KeyedCoProcessFunction<String, InnerMessage, InnerMessage, InnerMessage> {

    // maintain the alive tuple
    private ValueState<HashSet<InnerMessage>> alive;

    // Used for setting the key of the next message
    // maintain the counter
    private ValueState<Integer> counter;
    // maintain the last alive message
    private ValueState<InnerMessage> lastAliveMessage;

    // Used for debug information and state management
    private String functionName = "MixedProcessFunction";

    // group by: n_name,n2_name, l_year;
    private String[] nextKeys = {"n_name", "n2_name", "l_shipdate"};

    private Integer aliveCondition = 1;

    public MixedProcessFunction() {
        super();
    }

    private boolean finalPredicate(InnerMessage innerMessage) {

        String n_name = innerMessage.getValueByKey("n_name");
        String n2_name = innerMessage.getValueByKey("n2_name");
        assert n_name != null;
        assert n2_name != null;
        boolean and1 = n_name.equals("GERMANY") || n_name.equals("FRANCE");
        boolean and2 = n2_name.equals("GERMANY") || n2_name.equals("FRANCE");
        boolean and3 = !n_name.equals(n2_name);
        return and1 && and2 && and3;
    }

    // element1 and element2 are the same key (l_orderkey - o_orderkey)
    public void doProcessElement(InnerMessage innerMessage, Collector<InnerMessage> collector) throws Exception {
        switch (innerMessage.type) {
            case SET_ALIVE_RIGHT: {
                this.lastAliveMessage.update(innerMessage);
                Integer cnt = this.counter.value();
                this.counter.update(cnt + 1);
                HashSet<InnerMessage> aliveSet = this.alive.value();
                for (InnerMessage t : aliveSet) {
                    InnerMessage new_message = new InnerMessage(t, innerMessage);
                    new_message.setGroupKeyByKeyNames(nextKeys);
                    new_message.setType(AGGREGATE);
                    if (finalPredicate(new_message)) {
                        collector.collect(new_message);
                    }
                }
                break;
            }
            case SET_ALIVE_LEFT: {
                Integer aliveCounter = this.counter.value();
                HashSet<InnerMessage> aliveSet = this.alive.value();
                aliveSet.add(innerMessage);
                this.alive.update(aliveSet);
                if (aliveCounter.equals(aliveCondition)) {
                    // already alive status, update to top
                    InnerMessage lastAlive = this.lastAliveMessage.value();
                    InnerMessage new_message = new InnerMessage(lastAlive, innerMessage);
                    new_message.setGroupKeyByKeyNames(nextKeys);
                    new_message.setType(AGGREGATE);
                    if (finalPredicate(new_message)) {
                        collector.collect(new_message);
                    }
                }
                break;
            }
            case SET_DEAD_RIGHT: {
                this.lastAliveMessage.update(null);
                Integer cnt = this.counter.value();
                this.counter.update(cnt - 1);
                HashSet<InnerMessage> aliveSet = this.alive.value();
                for (InnerMessage t : aliveSet) {
                    InnerMessage new_message = new InnerMessage(t, innerMessage);
                    new_message.setGroupKeyByKeyNames(nextKeys);
                    new_message.setType(AGGREGATE_DELETE);
                    if (finalPredicate(new_message)) {
                        collector.collect(new_message);
                    }
                }
                break;
            }
            case SET_DEAD_LEFT: {
                Integer aliveCounter = this.counter.value();
                HashSet<InnerMessage> aliveSet = this.alive.value();
                aliveSet.remove(innerMessage);
                this.alive.update(aliveSet);
                if (aliveCounter.equals(aliveCondition)) {
                    // already alive status, update to top
                    InnerMessage lastAlive = this.lastAliveMessage.value();
                    InnerMessage new_message = new InnerMessage(lastAlive, innerMessage);
                    new_message.setGroupKeyByKeyNames(nextKeys);
                    new_message.setType(AGGREGATE_DELETE);
                    if (finalPredicate(new_message)) {
                        collector.collect(new_message);
                    }
                }
                break;
            }
            default:
                throw new Exception("Invalid operation type: " + innerMessage.type);
        }

    }

    // Used for update operations
    @Override
    public void processElement1(InnerMessage innerMessage, KeyedCoProcessFunction<String, InnerMessage, InnerMessage, InnerMessage>.Context context, Collector<InnerMessage> collector) throws Exception {
        initState();
        doProcessElement(innerMessage, collector);

    }

    // Used for input operations
    @Override
    public void processElement2(InnerMessage innerMessage, KeyedCoProcessFunction<String, InnerMessage, InnerMessage, InnerMessage>.Context context, Collector<InnerMessage> collector) throws Exception {
        initState();
        doProcessElement(innerMessage, collector);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        String functionName = this.functionName;
        counter = getRuntimeContext().
                getState(new ValueStateDescriptor<>(
                        functionName + "counter",
                        TypeInformation.of(Integer.class)
                ));

        alive = getRuntimeContext().
                getState(new ValueStateDescriptor<>(
                        functionName + "alive",
                        TypeInformation.of(new TypeHint<HashSet<InnerMessage>>() {
                        })
                ));
        lastAliveMessage = getRuntimeContext().
                getState(new ValueStateDescriptor<>(
                        functionName + "lastAliveMessage",
                        TypeInformation.of(InnerMessage.class)
                ));
    }


    // To avoid null pointer exceptions, initialize the state for the state that used it in an increment way
    private void initState() throws IOException {
        if (counter.value() == null) {
            counter.update(0);
        }
        if (alive.value() == null) {
            alive.update(new HashSet<>());
        }
    }
}


