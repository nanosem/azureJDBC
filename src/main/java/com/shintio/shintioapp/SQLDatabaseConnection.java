/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
// Use the JDBC driver
package com.shintio.shintioapp;

import java.sql.*;
import java.util.Random;
import java.util.Date;

public class SQLDatabaseConnection {

    private static Integer LAST_PRODUCT_ID = null;
    private static Integer LAST_ORDER_ID = null;
    
    public static void main(String[] args) {
        Connection connection = getConnection();
        try {
//            dropTable("ORDERS_POSITIONS", connection);
//            dropTable("PRODUCTS", connection);
//            dropTable("ORDERS", connection);

            createProductsTable(connection);
            createOrdersTable(connection);
            createOrdersPositionsTable(connection);
            
            writeToProductsTable(connection);
            writeToOrdersTable(connection);
            writeToOrdersPositionsTable(connection);
            
            selectFromTable("PRODUCTS", connection);
            selectFromTable("ORDERS", connection);
            selectFromTable("ORDERS_POSITIONS", connection);
            
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    public static Connection getConnection() {
        String connectionString = "jdbc:sqlserver://shintio.database.windows.net:1433;"
                + "database=SHOP_DB;"
                + "user=nanosem@shintio;"
//                + "password=password;"
                + "encrypt=true;"
                + "trustServerCertificate=false;"
                + "hostNameInCertificate=*.database.windows.net;"
                + "loginTimeout=30;";

        Connection connection = null;
        try {
            // Creating connection
            connection = DriverManager.getConnection(connectionString);
            System.out.println("Successfully got connection\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    // CREATE methods
    public static void createProductsTable(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            DatabaseMetaData dbm = connection.getMetaData();

            ResultSet tables = dbm.getTables(null, null, "PRODUCTS", null);
            if (!tables.next()) {
                // Table does not exist
                statement = connection.createStatement();
                String sql = "CREATE TABLE PRODUCTS "
                        + "(PRODUCTS_ID INTEGER IDENTITY NOT NULL, "
                        + " PRODUCTS_NAME VARCHAR(255), "
                        + " PRICE Integer, "
                        + " PRIMARY KEY ( PRODUCTS_ID ))";
                statement.execute(sql);
                System.out.println("Successfully created TABLE PRODUCTS\n");
            } else {
                System.out.println("TABLE PRODUCTS already exists \n");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void createOrdersTable(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            DatabaseMetaData dbm = connection.getMetaData();

            ResultSet tables = dbm.getTables(null, null, "ORDERS", null);
            if (!tables.next()) {
                // Table does not exist
                statement = connection.createStatement();
                String sql = "CREATE TABLE ORDERS "
                        + "(ORDERS_ID INTEGER IDENTITY NOT NULL, "
                        + " DESCRIPTION VARCHAR(255), "
                        + " ORDERS_DATE Date, "
                        + " TOTAL_COSTS Integer, "
                        + " CLIENTS_ID Integer, "
                        + " PRIMARY KEY ( ORDERS_ID ))";
                statement.execute(sql);
                System.out.println("Successfully created TABLE ORDERS\n");
            } else {
                System.out.println("TABLE ORDERS already exists\n");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void createOrdersPositionsTable(Connection connection) throws SQLException {
        Statement statement = null;
        try {
            DatabaseMetaData dbm = connection.getMetaData();

            ResultSet tables = dbm.getTables(null, null, "ORDERS_POSITIONS", null);
            if (!tables.next()) {
                // Table does not exist
                statement = connection.createStatement();
                String sql = "CREATE TABLE ORDERS_POSITIONS "
                        + "(ORDERS_POSITIONS_ID INTEGER IDENTITY NOT NULL, "
                        + " ORDERS_ID Integer, "
                        + " PRODUCTS_ID Integer, "
                        + " PRICE Integer, "
                        + " ITEM_COUNT Integer, "
                        + " PRIMARY KEY ( ORDERS_POSITIONS_ID ), "
                        + " FOREIGN KEY ( ORDERS_ID ) REFERENCES ORDERS( ORDERS_ID ) ON DELETE CASCADE, "
                        + " FOREIGN KEY ( PRODUCTS_ID ) REFERENCES PRODUCTS( PRODUCTS_ID ) ON DELETE CASCADE) ";
                statement.execute(sql);
                System.out.println("Successfully created TABLE ORDERS_POSITIONS\n");
            } else {
                System.out.println("TABLE ORDERS_POSITIONS already exists\n");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // INSERT methods
    public static void writeToProductsTable(Connection connection) {
        try {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("INSERT into PRODUCTS (PRODUCTS_NAME, PRICE) VALUES (?,?)",
                            Statement.RETURN_GENERATED_KEYS);

            Integer productRandomNumber = getRandomNumber(1, 99);
            Integer productPrice = getRandomNumber(1, 9999);
            preparedStatement.setString(1, "shintio_product_" + productRandomNumber);
            preparedStatement.setInt(2, productPrice);

            preparedStatement.executeUpdate();

            ResultSet tableKeys = preparedStatement.getGeneratedKeys();
            tableKeys.next();
            Integer autoGeneratedID = tableKeys.getInt(1);
            
            LAST_PRODUCT_ID = autoGeneratedID;
            
            System.out.println("successfully INSERT into PRODUCTS product with fields:");
            System.out.println("PRODUCTS_ID = " + autoGeneratedID);
            System.out.println("PRODUCTS_NAME = " + "shintio_product_" + productRandomNumber);
            System.out.println("PRICE = " + productPrice);
            System.out.println("\n----------------------------\n");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void writeToOrdersTable(Connection connection) throws SQLException {
        try {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("INSERT into ORDERS (DESCRIPTION, ORDERS_DATE, TOTAL_COSTS, CLIENTS_ID) VALUES (?,?, ?, ?)",
                            Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, "Lorem ipsum..");

            Date now = new Date();
            java.sql.Date sqlDate = new java.sql.Date(now.getTime());
            preparedStatement.setDate(2, sqlDate);

            int totalCosts = getRandomNumber(1, 9999);
            preparedStatement.setInt(3, totalCosts);

            int clientsId = getRandomNumber(1, 20);
            preparedStatement.setInt(4, clientsId);

            preparedStatement.executeUpdate();

            ResultSet tableKeys = preparedStatement.getGeneratedKeys();
            tableKeys.next();
            Integer autoGeneratedID = tableKeys.getInt(1);
            
            LAST_ORDER_ID = autoGeneratedID;
            
            System.out.println("successfully INSERT into ORDERS order with fields:");
            System.out.println("ORDERS_ID = " + autoGeneratedID);
            System.out.println("DESCRIPTION = Lorem ipsum..");
            System.out.println("ORDERS_DATE = " + sqlDate);
            System.out.println("TOTAL_COSTS = " + totalCosts);
            System.out.println("CLIENTS_ID = " + clientsId);
            System.out.println("\n----------------------------\n");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void writeToOrdersPositionsTable(Connection connection) throws SQLException {
        try {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("INSERT into ORDERS_POSITIONS (ORDERS_ID, PRODUCTS_ID, PRICE, ITEM_COUNT) VALUES (?,?, ?, ?)",
                            Statement.RETURN_GENERATED_KEYS);

            preparedStatement.setInt(1, LAST_ORDER_ID);
            preparedStatement.setInt(2, LAST_PRODUCT_ID);
            
            Integer price = getRandomNumber(1, 9999);
            preparedStatement.setInt(3, price);
            Integer itemsCount = getRandomNumber(1, 10);
            preparedStatement.setInt(4, itemsCount);

            preparedStatement.executeUpdate();

            ResultSet tableKeys = preparedStatement.getGeneratedKeys();
            tableKeys.next();
            Integer autoGeneratedID = tableKeys.getInt(1);
            System.out.println("successfully INSERT into ORDERS_POSITIONS ORDERS_POSITION with fields:");
            System.out.println("ORDERS_POSITIONS_ID = " + autoGeneratedID);
            System.out.println("ORDERS_ID = " + LAST_ORDER_ID);
            System.out.println("PRODUCTS_ID = " + LAST_PRODUCT_ID);
            System.out.println("PRICE = " + price);
            System.out.println("ITEM_COUNT = " + itemsCount);
            System.out.println("\n----------------------------\n");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // SELECT methods
    public static void selectFromTable(String tableName, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "SELECT * FROM " + tableName;
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) {
                    System.out.print(",  ");
                }
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
        System.out.println("");
    }

    // DROP methods
    public static void dropTable(String tableName, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "DROP TABLE " + tableName;
        statement.executeUpdate(sql);
        System.out.println("Table " + tableName + " deleted in given database...");
    }

    // Other methods
    private static int getRandomNumber(Integer fromValue, Integer toValue) {
        Random rand = new Random();
        int value = rand.nextInt(toValue) + fromValue;
        return value;
    }
}
