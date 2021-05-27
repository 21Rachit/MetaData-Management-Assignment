package com.metadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;


public class ReadDatabaseStructureExample {
	public static Connection conn;
	
	static Connection crunchifyConn = null;
	static PreparedStatement crunchifyPrepareStat = null;
	static ArrayList<String> tables=new ArrayList<String>();
	
	private static void log(String string) {
		System.out.println(string);
 
	}
	
	private static void makeTargetJDBCConnection() {
		 
		try {
			Class.forName("com.mysql.jdbc.Driver");
			log("Congrats - Seems your MySQL JDBC Driver Registered!");
		} catch (ClassNotFoundException e) {
			log("Sorry, couldn't found JDBC driver. Make sure you have added JDBC Maven Dependency Correctly");
			e.printStackTrace();
			return;
		}
 
		try {
			// DriverManager: The basic service for managing a set of JDBC drivers.
			crunchifyConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/shubham", "root", "Rashmiraj@0326");
			if (crunchifyConn != null) {
				log("Connection Successful! Enjoy. Now it's time to push data");
			} else {
				log("Failed to make connection!");
			}
		} catch (SQLException e) {
			log("MySQL Connection Failed!");
			e.printStackTrace();
			return;
		}
 
	}
	
	private static void makeSourceJDBCConnection() {
		 
		try {
			Class.forName("com.mysql.jdbc.Driver");
			log("Congrats - Seems your MySQL JDBC Driver Registered!");
		} catch (ClassNotFoundException e) {
			log("Sorry, couldn't found JDBC driver. Make sure you have added JDBC Maven Dependency Correctly");
			e.printStackTrace();
			return;
		}
 
		try {
			// DriverManager: The basic service for managing a set of JDBC drivers.
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/testnew", "root", "Rashmiraj@0326");
			if (conn != null) {
				log("Connection Successful! Enjoy. Now it's time to push data");
			} else {
				log("Failed to make connection!");
			}
		} catch (SQLException e) {
			log("MySQL Connection Failed!");
			e.printStackTrace();
			return;
		}
 
	}
	
	private static void dropMeta()
	{
		Statement stmt = null;
		try {
			stmt = crunchifyConn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String sql = "DROP TABLE shubham.metastore ";
	    try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void createMeta()
	{
		Statement stmt = null;
		try {
			stmt = crunchifyConn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String sql = "create table shubham.metastore("
				+ "id int not null auto_increment primary key,"
				+ "tableName varchar(1000),"
				+ "columnName varchar(1000),"
				+ "columnType varchar(1000),"
				+ "columnSize varchar(1000),"
				+ "ordinal varchar(1000),"
				+ "tablecatalog varchar(1000),"
				+ "data varchar(1000)"
				+ ")";
	    try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static void addDataToDB(String tableName,String columnName,String columnType,String columnSize,String ordinal,String tablecatalog,String data) {
		 
		try {
			String insertQueryStatement = "INSERT INTO metastore (tableName, columnName, columnType, columnSize, ordinal, tablecatalog, data) VALUES (?,?,?,?,?,?,?)";
 
			crunchifyPrepareStat = crunchifyConn.prepareStatement(insertQueryStatement);
			crunchifyPrepareStat.setString(1, tableName);
			crunchifyPrepareStat.setString(2, columnName);
			crunchifyPrepareStat.setString(3, columnType);
			crunchifyPrepareStat.setString(4, columnSize);
			crunchifyPrepareStat.setString(5, ordinal);
			crunchifyPrepareStat.setString(6, tablecatalog);
			crunchifyPrepareStat.setString(7, data);
 
			// execute insert SQL statement
			crunchifyPrepareStat.executeUpdate();
			log(columnName + " added successfully");
		} catch (
 
		SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void sourceLogic() throws ClassNotFoundException
	{
        try{
            DatabaseMetaData meta = conn.getMetaData();
 
            String catalog = null, schemaPattern = null, tableNamePattern = "%99";
            String[] types = {"TABLE"};
 
            ResultSet rsTables = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
 
 
            while (rsTables.next()) {
                String tableName = rsTables.getString(3);
                if(tables.contains(tableName)==false) {
                System.out.println("\n=== TABLE: " + tableName);
 
                String columnNamePattern = null;
                ResultSet rsColumns = meta.getColumns(catalog, schemaPattern, tableName, columnNamePattern);
 
                ResultSet rsPK = meta.getPrimaryKeys(catalog, schemaPattern, tableName);
                String columnNameAdd="",columnTypeAdd="",columnSizeAdd="",ordinalAdd="",tablecatalogAdd="",dataAdd="";
 
                while (rsColumns.next()) {
                    String columnName = rsColumns.getString("COLUMN_NAME");
                    String columnType = rsColumns.getString("TYPE_NAME");
                    int columnSize = rsColumns.getInt("COLUMN_SIZE");
                    int ordinal=rsColumns.getInt("ORDINAL_POSITION");
                    String tablecatalog=rsColumns.getString("TABLE_CAT");
                    int data=rsColumns.getInt("DATA_TYPE");
                    columnNameAdd+=columnName+", ";
                    columnTypeAdd+=columnType+", ";
                    columnSizeAdd+=columnSize+", ";
                    ordinalAdd+=ordinal+", ";
                    tablecatalogAdd+=tablecatalog+", ";
                    dataAdd+=data+", ";
                }
                addDataToDB(tableName,columnNameAdd,columnTypeAdd,columnSizeAdd,ordinalAdd,tablecatalogAdd,dataAdd);
                while (rsPK.next()) {
                    String primaryKeyColumn = rsPK.getString("COLUMN_NAME");
                    System.out.println("\tPrimary Key Column: " + primaryKeyColumn);
                }
                tables.add(tableName);
 
            }
            }
 
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }	
	}
	
	private static void info()
	{
		System.out.println("Enter 0 to enter data in meta table");
		System.out.println("Enter 1 for Meta Data Output");
		System.out.println("Enter 2 for add New Column");
		System.out.println("Enter 3 to update exisiting column");
		System.out.println("Enter 4 to delete a column");	
		System.out.println("Enter 5 to exit");
	}
	
    public static void main(String[] args) throws ClassNotFoundException {
    	
		makeTargetJDBCConnection();
		makeSourceJDBCConnection();
		Scanner sc=new Scanner(System.in);
		while(true) {
		info();
		int option=sc.nextInt();
		sc.nextLine();
		if(option==0)
		{
			sourceLogic();	
		}
		else if(option==1)
		{
			getDataFromMeta();	
		}
		else if(option==2)
		{
		System.out.println("Enter tableName");
		String table=sc.nextLine();
		System.out.println("Enter columnName");
		String column=sc.nextLine();
		addNewColumn(table,column);
		}
		else if(option==3)
		{
		System.out.println("Enter tableName");
		String table=sc.nextLine();
		System.out.println("Enter old columnName");
		String old=sc.nextLine();
		System.out.println("Enter new columnName");
		String newc=sc.nextLine();
		updateColumn(table,old,newc);
		}
		else if(option==4)
		{
		System.out.println("Enter tableName");
		String table=sc.nextLine();
		System.out.println("Enter column to delete");
		String del=sc.nextLine();
		deleteColumn(table,del);
		}
		else
			break;
		}

    }
    
    private static void addNewColumn(String tableName,String columnName)
    {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String sql = "ALTER TABLE "+tableName+ " ADD COLUMN " +columnName+" "+" varchar(45) ";
		//System.out.println(sql);
	
	    try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    
	    dropMeta();
	    createMeta();	
    }
    
    private static void updateColumn(String tableName, String columnName,String newcolumnName)
    {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String sql = "ALTER TABLE "+tableName+ " CHANGE " +columnName+" "+newcolumnName+" "+" varchar(45) ";
		//System.out.println(sql);
	
	    try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    
	    dropMeta();
	    createMeta();	
    }
    
    private static void deleteColumn(String tableName, String columnName)
    {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String sql = "ALTER TABLE "+tableName+ " DROP COLUMN " +columnName;
	
	    try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	    
	    dropMeta();
	    createMeta();
	    
    }
    
	private static void getDataFromMeta() {
		 
		try {
			// MySQL Select Query Tutorial
			String getQueryStatement = "SELECT * FROM shubham.metastore";
 
			crunchifyPrepareStat = crunchifyConn.prepareStatement(getQueryStatement);
 
			// Execute the Query, and get a java ResultSet
			ResultSet rs = crunchifyPrepareStat.executeQuery();
 
			// Let's iterate through the java ResultSet
			while (rs.next()) {
				String tableName = rs.getString("tableName");
				String columnName = rs.getString("columnName");
				String columnType = rs.getString("columnType");
				String columnSize = rs.getString("columnSize");
				String ordinal = rs.getString("ordinal");
				String tablecatalog = rs.getString("tablecatalog");
				String data = rs.getString("data");
				
				System.out.println("tableName "+tableName);
				System.out.println("columnName "+columnName);
				System.out.println("columnType "+columnType);
				System.out.println("columnSize "+columnSize);
				System.out.println("ordinal "+ordinal);
				System.out.println("tablecatalog "+tablecatalog);
				System.out.println("data "+data);
				System.out.println();
				
			}
 
		} catch (
 
		SQLException e) {
			e.printStackTrace();
		}
 
	}
	
    
    
}