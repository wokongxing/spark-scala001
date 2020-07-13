package xiaolin.json;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.hive.jdbc.HiveDriver;

/**
 * hive api测试
 * @author linzhy
 */
public class JaveHIveApi {

    public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection connection = DriverManager.getConnection("jdbc:hive2://hadoop001:10000", "hive", "123456");
            Statement stmt = connection.createStatement();
            String querySQL="show tables";
            ResultSet resut = stmt.executeQuery(querySQL);
            while (resut.next()) {
                System.out.println(resut.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
