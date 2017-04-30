import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class DatabaseHandler {
    private String url;
    private String user;
    private String password;

    private Connection con;
    private Statement stmt;
    private ResultSet rs;

    private static final int DATE_COLUMN = 1;
    private static final int MSEC_COLUMN = 2;
    private static final int VALUE_COLUMN = 3;
    private static final int NUMBER_OF_VALUES_IN_TABLE = 36;
    private static final int NUMBER_OF_SIGNAL_INDEX = 11;

    public DatabaseHandler(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public ArrayList<Point> getAkhz() {
        final int firstTablePostfix = 52;
        final int lastTablePostfix = 61;

        ArrayList<Point> akhz1 = new ArrayList<Point>();
        FillTable(akhz1, "akhz1_data", firstTablePostfix, lastTablePostfix, 1);

        akhz1.sort(new TimeComparator());

        return akhz1;
    }

    private void FillTable(ArrayList<Point> toList, String tableNamePrefix, int firstTablePostfix, int lastTablePostfix, int signalIndex ) {
        for (int currentTablePostfix = firstTablePostfix; currentTablePostfix <= lastTablePostfix; currentTablePostfix++) {
            for (int currentNumberOfValues = 1; currentNumberOfValues <= NUMBER_OF_VALUES_IN_TABLE; currentNumberOfValues++) {
                for (int currentSignalIndex = 1; currentSignalIndex <= signalIndex; currentSignalIndex++) {

                    String query = String.format("SELECT Sample_TDate_%d, Sample_MSec_%d, Sample_Value_%d FROM %s_%d WHERE Signal_Index=%d",
                            currentNumberOfValues, currentNumberOfValues, currentNumberOfValues, tableNamePrefix, currentTablePostfix, currentSignalIndex);

                    copyPointsFromTable(query, toList);
                }
            }
        }
    }

    public ArrayList<LiftedPoint> getCircle() {
        int count = 1;
        int circle = 0;

        ArrayList<Point> akhz1 = getAkhz();
        ArrayList<LiftedPoint> liftedPoints = new ArrayList<LiftedPoint>();

        for (int i = 1; i <= akhz1.size() - 2; i++) {
            if (circle < 160) {
                float temp = akhz1.get(i).getValue();
                long time = akhz1.get(i).getTime();
                float delta_old = akhz1.get(i).getValue() - akhz1.get(i - 1).getValue();
                float delta_new = akhz1.get(i + count).getValue() - akhz1.get(i).getValue();
                if (temp < -520 && (delta_new > 0 && delta_old < 0)) {
                    liftedPoints.add(new LiftedPoint(akhz1.get(i), true));// up
                    circle++;
                } else if (temp > 50 && (delta_new < 0 && delta_old > 0)) {
                    liftedPoints.add(new LiftedPoint(akhz1.get(i), false));// down
                    circle++;
                }
            }
        }
        return liftedPoints;
    }


    public List<TreeMap<Long, Float>> getTemperature() {
        final int firstTablePostfix = 52 ;
        final int lastTablePostfix = 62;

        ArrayList<Float> rowss = new ArrayList<Float>();
        List<TreeMap<Long, Float>> res = new ArrayList<TreeMap<Long, Float>>();

        ArrayList<Point> temperature = new ArrayList<Point>();
        FillTable(temperature, "pressdrv", firstTablePostfix, lastTablePostfix, 11);

        temperature.sort(new TimeComparator());

        ArrayList<LiftedPoint> list = getCircle();
        for (int i = 0; i < list.size() - 1; i++) {
            LiftedPoint instance = list.get(i);

            long timeOfPosition1 = list.get(i).getPoint().getTime();
            long timeOfPosition2 = list.get(i + 1).getPoint().getTime();

            for (int j = 0; j < temperature.size(); j++) {
                long timeOfTemperature = temperature.get(j).getTime();
                    if ((instance.isUpwards()) && (timeOfPosition1 <= timeOfTemperature && timeOfTemperature <= timeOfPosition2)) {

                    rowss.add(temperature.get(j).getValue());
                    TreeMap<Long, Float> treeMap = new TreeMap<Long, Float>();
                    treeMap.put(temperature.get(j).getTime(),  temperature.get(j).getValue());
                    res.add(treeMap);
                }
            }
        }
        return res;
    }

    private void copyPointsFromTable(String query, ArrayList<Point> toList) {
        try {
            con = DriverManager.getConnection(url, user, password);
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);

            while (rs.next()) {
                long dateInMs=0;
                long timeInMs=0;
                java.util.Date curentDate = rs.getDate(DATE_COLUMN);
                java.util.Date timeInMsec = rs.getTime(DATE_COLUMN);


                if (curentDate == null && timeInMsec == null){
                    continue;}
                    else {
                    dateInMs = curentDate.getTime();
                    timeInMs = timeInMsec.getTime();
                    long ms = rs.getInt(MSEC_COLUMN);
                    long time = dateInMs + timeInMs + ms;
                    float value = rs.getFloat(VALUE_COLUMN);

                    toList.add(new Point(time, value));
                }
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (SQLException se) {
            }
            try {
                stmt.close();
            } catch (SQLException se) {
            }
            try {
                rs.close();
            } catch (SQLException se) {
            }
        }
    }
}
