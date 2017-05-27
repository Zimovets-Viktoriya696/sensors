import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public Map<Integer, List<Point>> readData (String nameTable, int SignalIndexQ){
        String query = "";
        if(SignalIndexQ == 0){
            query = String.format("SELECT * FROM %s", nameTable);
        }
        else {
            query = String.format("SELECT * FROM %s WHERE Signal_Index = %d" , nameTable, SignalIndexQ);
        }
        Map<Integer, List<Point>> data = new TreeMap<>();
        try {
            con = DriverManager.getConnection(url, user, password);
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                long dateInMs=0;
                long timeInMs=0;
                int SignalIndex = rs.getInt("Signal_Index");
                for (int i = 1; i <= 36; i++) {
                    java.util.Date curentDate = rs.getDate("Sample_TDate_" + i);
                    java.util.Date timeInMsec = rs.getTime("Sample_TDate_" + i);
                    float value = rs.getFloat("Sample_Value_" + i);

                    if (curentDate != null && timeInMsec != null){
                        if(!data.containsKey(SignalIndex)) {
                            data.put(SignalIndex, new ArrayList<>());}

                            dateInMs = curentDate.getTime();
                            timeInMs = timeInMsec.getTime();
                            long ms = rs.getInt("Sample_MSec_" + i);
                            long time = dateInMs + timeInMs + ms;
                            data.get(SignalIndex).add(new Point(time, value));

                    }
                }
            }
        }
        catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException se) {
            }
            try {
                stmt.close();
            } catch (SQLException se) {
            }
            try {
                con.close();
            } catch (SQLException se) {
            }
        }
        return data;
    }

    public Map<Integer, List<Point>> readAllData(int firstNumerTable, int lastNumberTable, String nameTable, int SignalIndex){
        Map<Integer, List<Point>> data = new TreeMap<>();
        for (int i = firstNumerTable; i < lastNumberTable; i++) {
            Map<Integer, List<Point>> tableData = readData(nameTable + i, SignalIndex);
            for (Map.Entry<Integer, List<Point>> entry: tableData.entrySet()){
                int key = entry.getKey();
                if(data.containsKey(key)){
                    data.get(key).addAll(entry.getValue());
                }
                else {
                    data.put(key, new ArrayList<>(entry.getValue()));
                }
            }
        }
        return data;
    }

    public List<Period> getPeriods () {
        List<Period> resultPosition = new ArrayList<>();
        Map<Integer, List<Point>> position = readAllData(50, 54, "akhz1_data_", 1);
        int count = 1;
        int circle = 1;
        long timeUp = 0;
        long timeDown = 0;
        boolean direction = false;
        for (Map.Entry<Integer, List<Point>> entry : position.entrySet()) {
            for (int i = 1; i < entry.getValue().size()-1; i++) {
                //if (circle < 160) {
                float temp = entry.getValue().get(i).getValue();
                float previous = entry.getValue().get(i - 1).getValue();
                float next = entry.getValue().get(i + 1).getValue();
                long time = entry.getValue().get(i).getTime();
                float delta_old = temp - previous;
                float delta_new = next - temp;
                if (temp < -520 && (delta_new > 0 && delta_old < 0)) {
                    timeUp = time;
                    direction = true;// up
                    circle++;
                }
                else if (temp > 50 && (delta_new < 0 && delta_old > 0)) {
                    timeDown = time;
                    direction = true;// up
                    circle++;
                }
                else if (temp < -520 && (delta_new < 0 && delta_old < 0)) {
                    timeUp = time;
                    direction = false;// down
                    circle++;
                }
                else if (temp > 50 && (delta_new > 0 && delta_old < 0)) {
                    timeDown = time;
                    direction = false;// down
                    circle++;
                }
                if (timeUp != 0 || timeDown != 0){
                resultPosition.add(new Period(timeUp, timeDown, direction));
                timeUp = 0;
                timeDown = 0;
                direction = false;
                }
            }
        }
        return resultPosition;
    }

    //public Map<Integer, List<Point>> parseData(){}


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
                rs.close();
            } catch (SQLException se) {
            }
            try {
                stmt.close();
            } catch (SQLException se) {
            }
            try {
                con.close();
            } catch (SQLException se) {
            }
        }
    }
}
