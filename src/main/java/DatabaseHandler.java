import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class DatabaseHandler {
    private String _url;
    private String _user;
    private String _password;

    private Connection _con;
    private Statement _stmt;
    private ResultSet _rs;

    private static final int DATE_COLUMN = 1;
    private static final int MSEC_COLUMN = 2;
    private static final int VALUE_COLUMN = 3;
    private static final int NUMBER_OF_VALUES_IN_TABLE = 36;

    public DatabaseHandler(String url, String user, String password) {
        _url = url;
        _user = user;
        _password = password;
    }

    public ArrayList<Point> GetAkhz1() {
        final int firstTablePostfix = 52;
        final int lastTablePostfix = 61;

        ArrayList<Point> akhz1 = new ArrayList<Point>();
        FillTable(akhz1, "akhz1_data", firstTablePostfix, lastTablePostfix);

        akhz1.sort(new TimeComparator());

        return akhz1;
    }

    private void FillTable(ArrayList<Point> toList, String tableNamePrefix, int firstTablePostfix, int lastTablePostfix) {
        for (int currentTablePostfix = firstTablePostfix; currentTablePostfix <= lastTablePostfix; currentTablePostfix++) {
            for (int currentNumberOfValues = 1; currentNumberOfValues <= NUMBER_OF_VALUES_IN_TABLE; currentNumberOfValues++) {
                String query = String.format("SELECT Sample_TDate_%d, Sample_MSec_%d, Sample_Value_%d FROM %s_%d WHERE Signal_Index=1",
                        currentNumberOfValues, currentNumberOfValues, currentNumberOfValues, tableNamePrefix, currentTablePostfix);

                CopyPointsFromTable(query, toList);
            }
        }
    }


    public ArrayList<Point> GetCircle() {
        int count = 1;
        ArrayList<Point> akhz1 = new ArrayList<Point>();
        FillTable(akhz1, "akhz1_data", 50, 61);
        int circle = 0;
        ArrayList<Point> down = new ArrayList<Point>();


        for (int i = 1; i < akhz1.size() - 2; i++) {
            if (circle < 160) {
                float temp = akhz1.get(i).getValue();
                float delta_old = akhz1.get(i).getValue() - akhz1.get(i - 1).getValue();
                float delta_new = akhz1.get(i + count).getValue() - akhz1.get(i).getValue();
                if (temp > 48.0 && (delta_new < 0 && delta_old > 0)) {

                    down.add(akhz1.get(i));//конец подъома
                    circle++;
                } else if (temp < -520 && (delta_new > 0 && delta_old < 0)) {
                    down.add(akhz1.get(i));
                    // down.add(akhz1.get(i).getTime());//начало подъема
                    circle++;
                }
                   /* else if (temp > 48 && (delta_new < 0 && delta_old > 0)) {
                        down.add(akhz1.get(i));
                       // down.add(akhz1.get(i).getTime());// начало спуска
                        circle++;
                    }
                    else if (temp < -520 && (delta_new < 0 && delta_old > 0)) {
                        down.add(akhz1.get(i));
                        //down.add(akhz1.get(i).getTime()); // конец спуска
                        circle++;
                    }*/
            }
        }
        return down;
    }

    public ArrayList<Flow> GetCircle1() {
        int count = 1;
        ArrayList<Point> akhz1 = new ArrayList<Point>();
        FillTable(akhz1, "akhz1_data", 51, 61);
        int circle = 0;

        ArrayList<Flow> flow = new ArrayList<Flow>();
        ArrayList<LiftedPoint> liftedPoints = new ArrayList<LiftedPoint>();

        for (int i = 1; i < akhz1.size() - 2; i++) {
            if (circle < 160) {
                float temp = akhz1.get(i).getValue();
                long time = akhz1.get(i).getTime();
                float delta_old = akhz1.get(i).getValue() - akhz1.get(i - 1).getValue();
                float delta_new = akhz1.get(i + count).getValue() - akhz1.get(i).getValue();
                if (temp < -520 && (delta_new > 0 && delta_old < 0)) {
                    liftedPoints.add(new Point(time, temp), true);
                    flow.add(new LiftUp(time, temp));// подъом
                    circle++;
                } else if (temp > 50 && (delta_new < 0 && delta_old > 0)) {
                    flow.add(new LiftDown(time, temp));
                    // down.add(akhz1.get(i).getTime());//спуск
                    circle++;
                }
            }
        }
        return flow;
    }


    public ArrayList<Float> getTemperature() {
        final int firstTablePostfix = 52 ;
        final int lastTablePostfix = 62;

        ArrayList<Float> rowss = new ArrayList<Float>();
        List<TreeMap<Long, Float>> res = new ArrayList<TreeMap<Long, Float>>();

        ArrayList<Point> temperature = new ArrayList<Point>();
        System.out.println(temperature.size() + "+++++++++++++size");
        FillTable(temperature, "pressdrv", firstTablePostfix, lastTablePostfix);

        temperature.sort(new TimeComparator());

        ArrayList<Flow> list = GetCircle1();
        for (int i = 0; i < 20; i++) {
            Flow instance1 = list.get(i);
            Flow instance2 = list.get(i + 1);

            long timeOfPosition1 = list.get(i).getTime();
            long timeOfPosition2 = list.get(i + 1).getTime();

            for (int j = 0; j < temperature.size(); j++) {
                long timeOfTemperature = temperature.get(j).getTime();
                    if ((instance1 instanceof LiftUp && instance2 instanceof LiftDown) && (timeOfPosition1 <= timeOfTemperature && timeOfTemperature <= timeOfPosition2)) {

                    rowss.add(temperature.get(j).getValue());
                    TreeMap<Long, Float> treeMap = new TreeMap<Long, Float>();
                    treeMap.put(temperature.get(j).getTime(),  temperature.get(j).getValue());
                    res.add(treeMap);
                    }
            }
        }
        return rowss;
    }

    private void CopyPointsFromTable(String query, ArrayList<Point> toList) {
        try {
            _con = DriverManager.getConnection(_url, _user, _password);
            _stmt = _con.createStatement();
            _rs = _stmt.executeQuery(query);

            while (_rs.next()) {
                try {
                    long dateInMs = _rs.getDate(DATE_COLUMN).getTime();
                    long timeInMs = _rs.getTime(DATE_COLUMN).getTime();
                    long ms = _rs.getInt(MSEC_COLUMN);

                    long time = dateInMs + timeInMs + ms;
                    float value = _rs.getFloat(VALUE_COLUMN);

                    toList.add(new Point(time, value));
                } catch (NullPointerException nullEx) {
                    nullEx.printStackTrace();
                }
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        } finally {
            try {
                _con.close();
            } catch (SQLException se) {
            }
            try {
                _stmt.close();
            } catch (SQLException se) {
            }
            try {
                _rs.close();
            } catch (SQLException se) {
            }
        }
    }
}