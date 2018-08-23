package com.att.research.mdbc.examples;

import java.sql.*;
import org.apache.calcite.avatica.remote.Driver;

public class EtdbTestClient {

    public static class Hr {
        public final Employee[] emps = {
                new Employee(100, "Bill"),
                new Employee(200, "Eric"),
                new Employee(150, "Sebastian"),
        };
    }

    public static class Employee {
        public final int empid;
        public final String name;

        public Employee(int empid, String name) {
            this.empid = empid;
            this.name = name;
        }
    }

    public static void main(String[] args){
        try {
            Class.forName("org.apache.calcite.avatica.remote.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection connection;
        try {
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:30000;serialization=protobuf");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

// Some non-user-derived input
        final String sql = "CREATE TABLE IF NOT EXISTS Persons (\n" +
                "    PersonID int,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");";
        Statement stmt;
        try {
            stmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        boolean execute;
        try {
            execute = stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        if (execute) {
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

//        SchemaPlus rootSchema = calciteConnection.getRootSchema();
//        rootSchema.add("hr", new ReflectiveSchema(new Hr()));
//        Statement statement = null;
//        try {
//            statement = connection.createStatement();
//        } catch (SQLException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//        ResultSet resultSet =
//                null;
//        try {
//            resultSet = statement.executeQuery("select *\n"
//                    + "from \"hr\".\"emps\" as e\n");
//        } catch (SQLException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//        final StringBuilder buf = new StringBuilder();
//        while (resultSet.next()) {
//            int n = 0;
//            try {
//                n = resultSet.getMetaData().getColumnCount();
//            } catch (SQLException e) {
//                e.printStackTrace();
//                continue;
//            }
//            for (int i = 1; i <= n; i++) {
//                buf.append(i > 1 ? "; " : "")
//                        .append(resultSet.getMetaData().getColumnLabel(i))
//                        .append("=")
//                        .append(resultSet.getObject(i));
//            }
//            System.out.println(buf.toString());
//            buf.setLength(0);
//        }
//        resultSet.close();
//        statement.close();
//        connection.close();
//    }
    }
}
