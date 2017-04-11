import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by Vika on 26.03.2017.
 */
public class Rows {
    private TreeMap<Long, Float> listOfValues;

    public Rows(Float put) {
    }

    public TreeMap<Long, Float> getRow(){return listOfValues;}


    Rows( TreeMap<Long, Float> row) {
        this.listOfValues = row;
    }

    public Rows getRow (Long time, Float value){

        Rows row = new Rows(listOfValues.put(time, value));
       return row;
    }
}
