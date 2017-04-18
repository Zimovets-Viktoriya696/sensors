import java.util.Comparator;

public class Point
{
    private long time;
    public long getTime() { return time; }
    private float value;
    public float getValue() { return value; }

    public Point(long time, float value)
    {
        this.time = time;
        this.value = value;
    }

    @Override
    public String toString()
    {
        return String.format("{time=%d, value=%f}", time, value);
    }
}

