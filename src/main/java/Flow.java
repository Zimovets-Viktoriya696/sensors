import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Виктория on 04.06.16.
 */
public class Flow {

    private long _time;
    public long getTime() { return _time; }
    private float _value;
    public double getValue() { return _value; }


        Flow(long time, float value ) {
            this._time=time;
            this._value= value;
        }





}
class TimeComparatorFor implements Comparator<Flow>
{

    @Override
    public int compare(Flow a, Flow b) {
        return a.getTime() < b.getTime() ? -1 : a.getTime() == b.getTime() ? 0 : 1;
    }
}
