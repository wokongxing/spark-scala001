package com.xiaolin.sparksql.work;

import java.sql.*;
import java.util.Properties;

/**
 * @auther 林
 * @Date 2019/10/21 17:28
 **/
public class PrestoTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "null");
        properties.setProperty("SSL", "true");

        Connection connection = DriverManager.getConnection("jdbc:presto://hadoop001:8080/hive/default","root",null);
        Statement statement = connection.createStatement();
        String sql = "SHOW SCHEMAS FROM mysql";
        String sql2 = "SHOW tables ";
        ResultSet resultSet = statement.executeQuery(sql2);
        while (resultSet.next()){
            System.out.println(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        connection.close();

    }
}
