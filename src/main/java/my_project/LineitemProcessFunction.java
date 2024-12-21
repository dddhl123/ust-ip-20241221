package my_project;

import java.time.LocalDate;

public class LineitemProcessFunction extends NotLeafProcessFunction {
    public LineitemProcessFunction() {
        super("l_orderkey", 1, "LineitemProcessFunction", InnerMessage.OperationType.SET_ALIVE_LEFT, InnerMessage.OperationType.SET_DEAD_LEFT);
    }

    @Override
    public boolean predicate(InnerMessage innerMessage) {
        String l_shipdate = innerMessage.getValueByKey("l_shipdate");
        assert l_shipdate != null;
        // Parse the date
        LocalDate date = LocalDate.parse(l_shipdate);
        return date.isAfter(LocalDate.parse("1995-01-01")) && date.isBefore(LocalDate.parse("1996-12-31"));
    }

}
