import java.util.Comparator;

/**
 * Created by Vika on 18.04.2017.
 */
public class TimeComparator implements Comparator<Point>
{

    @Override
    public int compare(Point a, Point b) {
        return a.getTime() < b.getTime() ? -1 : a.getTime() == b.getTime() ? 0 : 1;
    }
}


