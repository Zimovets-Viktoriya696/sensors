import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Program {
    private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/db";
    private static final String DATABASE_USERNAME = "root";
    private static final String DATABASE_PASSWORD = "Digital8";

    public static void main(String[] args) {
        DatabaseHandler database = new DatabaseHandler(DATABASE_URL, DATABASE_USERNAME, DATABASE_PASSWORD);

        //   ArrayList<Point> akhz1 = database.GetAkhz1();
        // ArrayList<Float> list = database.getTemperature();
        //   List<TreeMap<Long, Float>> list = database.getTemperature();
      //  List<Period> period = database.getPeriods();
        /*for (int i = 0; i < period.size(); i++) {
            System.out.println(period.get(i).getLeft() + "left " + period.get(i).getRight() + " right");
        }*/
       // Map<Integer, List<Point>> list = database.readAllData(50, 54, "akhz1_data_", 1);
        List<Period> list = database.getPeriods();


         for (int i = 0; i < list.size() - 1; i++) {
            /*for (Map.Entry entry : list.entrySet()) {
                System.out.println(entry.getKey() + " " + entry.getValue());*/

                 System.out.println(i + " " + list.get(i).getRight()  + " right " + list.get(i).getLeft() + " left " + list.get(i).isDirection());
            }
            //   ArrayList<Long> up = database.fotyUp();

     /* for (int i = 0; i < akhz1.size(); i++) {
           System.out.println(up.get(i) +" up" );

        }*/
        }
    }
