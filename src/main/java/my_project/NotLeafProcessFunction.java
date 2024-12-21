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

public abstract class NotLeafProcessFunction extends KeyedCoProcessFunction<String, InnerMessage, InnerMessage, InnerMessage> {

    // maintain the alive tuple
    private ValueState<HashSet<InnerMessage>> alive;

    // Used for setting the key of the next message
    // maintain the counter
    private ValueState<Integer> counter;
    // maintain the last alive message
    private ValueState<InnerMessage> lastAliveMessage;

    // Used for debug information and state management
    private String functionName;

    private String nextKey;

    // alive condition equals to the children number
    private Integer aliveCondition;

    private InnerMessage.OperationType setAliveOperationType;
    private InnerMessage.OperationType setDeadOperationType;

    public NotLeafProcessFunction(String nextKey, Integer aliveCondition, String functionName) {
        super();
        this.nextKey = nextKey;
        this.aliveCondition = aliveCondition;
        this.functionName = functionName;
        this.setAliveOperationType = InnerMessage.OperationType.SET_ALIVE;
        this.setDeadOperationType = InnerMessage.OperationType.SET_DEAD;
    }

    public NotLeafProcessFunction(String nextKey, Integer aliveCondition, String functionName, InnerMessage.OperationType setAliveOperationType, InnerMessage.OperationType setDeadOperationType) {
        super();
        this.nextKey = nextKey;
        this.aliveCondition = aliveCondition;
        this.functionName = functionName;
        this.setAliveOperationType = setAliveOperationType;
        this.setDeadOperationType = setDeadOperationType;
    }


    public boolean predicate(InnerMessage innerMessage) {
        return true;
    }

    // Used for update operations
    @Override
    public void processElement1(InnerMessage innerMessage, KeyedCoProcessFunction<String, InnerMessage, InnerMessage, InnerMessage>.Context context, Collector<InnerMessage> collector) throws Exception {
        initState();
        switch (innerMessage.type) {
            case SET_ALIVE: {
                this.lastAliveMessage.update(innerMessage);
                this.counter.update(this.counter.value() + 1);
                HashSet<InnerMessage> aliveSet = this.alive.value();
                for (InnerMessage t : aliveSet) {
                    InnerMessage new_message = new InnerMessage(t, innerMessage);
                    new_message.setKeyByKeyName(nextKey);
                    new_message.setType(setAliveOperationType);
                    collector.collect(new_message);
                }
                break;
            }
            case SET_DEAD:{
                this.lastAliveMessage.update(null);
                this.counter.update(this.counter.value() - 1);
                HashSet<InnerMessage> aliveSet = this.alive.value();
                for (InnerMessage t : aliveSet) {
                    InnerMessage new_message = new InnerMessage(t, innerMessage);
                    new_message.setKeyByKeyName(nextKey);
                    new_message.setType(setDeadOperationType);
                    collector.collect(new_message);
                }
                break;
            }
            default:
                throw new Exception("Invalid operation type: " + innerMessage.type);
        }
    }

    // Used for input operations
    @Override
    public void processElement2(InnerMessage innerMessage, KeyedCoProcessFunction<String, InnerMessage, InnerMessage, InnerMessage>.Context context, Collector<InnerMessage> collector) throws Exception {
        initState();
        switch (innerMessage.type) {
            case INSERT: {
                if (!predicate(innerMessage)) {
                    return;
                }
                Integer aliveCounter = this.counter.value();
                HashSet<InnerMessage> aliveSet = this.alive.value();
                aliveSet.add(innerMessage);
                this.alive.update(aliveSet);
                if (aliveCounter.equals(aliveCondition)) {
                    // already alive status, update to top
                    InnerMessage lastAlive = this.lastAliveMessage.value();
                    InnerMessage new_message = new InnerMessage(lastAlive, innerMessage);
                    new_message.setKeyByKeyName(nextKey);
                    new_message.setType(setAliveOperationType);
                    collector.collect(new_message);
                }
                break;
            }
            case DELETE: {
                if (!predicate(innerMessage)) {
                    return;
                }
                if(this.counter.value().equals(aliveCondition)) {
                    // already alive status, update to top to make it dead
                    InnerMessage lastAlive = this.lastAliveMessage.value();
                    InnerMessage new_message = new InnerMessage(lastAlive, innerMessage);
                    new_message.setKeyByKeyName(nextKey);
                    new_message.setType(setDeadOperationType);
                    collector.collect(new_message);
                }
                HashSet<InnerMessage> aliveSet = this.alive.value();
                aliveSet.remove(innerMessage);
                this.alive.update(aliveSet);
                break;
            }
            default:
                throw new Exception("Invalid operation type: " + innerMessage.type);
        }
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


