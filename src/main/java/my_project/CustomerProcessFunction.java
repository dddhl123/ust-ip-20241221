package my_project;

public class CustomerProcessFunction extends NotLeafProcessFunction {
    public CustomerProcessFunction() {
        super("c_custkey", 1, "CustomerProcessFunction");
    }
}

