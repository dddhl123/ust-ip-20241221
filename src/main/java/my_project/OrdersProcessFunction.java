package my_project;

public class OrdersProcessFunction extends NotLeafProcessFunction {
    public OrdersProcessFunction() {
        super("o_orderkey", 1, "OrdersProcessFunction", InnerMessage.OperationType.SET_ALIVE_RIGHT, InnerMessage.OperationType.SET_DEAD_RIGHT);
    }
}
