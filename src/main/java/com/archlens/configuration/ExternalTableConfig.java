package com.archlens.configuration;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hive.service.cli.HiveSQLException;
import org.slf4j.Logger;

import com.archlens.App;
import com.archlens.security.ArchLensSecurity;
import com.archlens.service.ArchLensService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//@Configuration
public class ExternalTableConfig {

	static Logger log = App.log;

	static String filePath = ArchLensService.DATA_SOURCE;

	// Method to Get a list of schemas present in Data Source
	public static List<Object> getSchemas(Connection connection) throws Exception {

		String query = "SHOW SCHEMAS";

		List schemas = null;

		try {

			Statement stmt = connection.createStatement();
			log.info("Excecuting query : " + query);
			ResultSet resultSet = stmt.executeQuery(query);

			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();

			schemas = new ArrayList<Object>();
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					Object value = resultSet.getObject(i);
					schemas.add(value);
				}
			}

		} catch (SQLException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		log.info("Successfully fetched the SCHEMAS present in Data Source");
		return schemas;
	}

	// Method to Get a list of schemas present in Data Source
	public static List<String> getSchemas(String dataSource) throws SQLException, IOException, ClassNotFoundException {

		String query = "SHOW SCHEMAS";
		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance to read JSON file
		ObjectMapper objectMapper = new ObjectMapper();
		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new IOException("Failed to read property.json");
		}

		JsonNode dataSourceNode = rootNode.get(dataSource);
		
		if (dataSourceNode == null) {
			throw new IOException(dataSource + " Data Source not found");
		}

		connectionURL = dataSourceNode.get("connectionURL").asText();
		username = dataSourceNode.get("username").asText();
		password = dataSourceNode.get("password").asText();

		// Decrypting UserName and Password
		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection connection = DriverManager.getConnection(connectionURL, username, password);
			log.info("Successfully created connection to : " + connectionURL);
			Statement stmt = connection.createStatement();
			log.info("Excecuting query : " + query);
			ResultSet resultSet = stmt.executeQuery(query);

			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();

			List<String> schemas = new ArrayList<>();
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					Object value = resultSet.getObject(i);
					schemas.add((String) value);
				}
			}

			return schemas;
		} catch (SQLException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
	}

	public static List<Object> getTables(Connection connection) throws Exception {
		String query = "SHOW Tables";
		List<Object> tables = new ArrayList<Object>();
		try {
			Statement stmt = connection.createStatement();
			log.info("Excecuting query : " + query);
			ResultSet resultSet = stmt.executeQuery(query);

			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();

			while (resultSet.next()) {

				for (int i = 2; i <= columnCount; i++) {
					Object value = resultSet.getObject(i);
					tables.add(value);
					break;
				}
			}

		} catch (SQLException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		log.info("Successfully fetched the Tables present in Schema");
		return tables;
	}

// 	Method to get a list Tables present in Schema
	public static List<String> getTables(String config, String schema) throws Exception {
		String query = "SHOW Tables";

		String connectionURL = null, username = null, password = null;
		// Create an ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();
		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));
		} catch (IOException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new IOException("Failed to read property.json");

		}

		// Access each data source object and retrieve the connectionURL, username, and
		// password
		JsonNode dataSource = rootNode.get(config);
		if (dataSource == null) {
			throw new IOException(config + " Data Source not found");
		}

		connectionURL = dataSource.get("connectionURL").asText();
		username = dataSource.get("username").asText();
		password = dataSource.get("password").asText();
		connectionURL = connectionURL + "/" + schema;

		// Decrypting User name and Password
		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		try {
			Connection connection = DriverManager.getConnection(connectionURL, username, password);
			log.info("Successfully created connection to : " + connectionURL);
			Statement stmt = connection.createStatement();
			log.info("Excecuting query : " + query);
			ResultSet resultSet = stmt.executeQuery(query);
			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();

			List<String> tables = new ArrayList<String>();

			while (resultSet.next()) {
				for (int i = 2; i <= columnCount; i++) {
					Object value = resultSet.getObject(i);
					tables.add((String) value);
					break;
				}
			}
			return tables;

		} catch (SQLException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

	}

//	Method to Create a Connection to a Schema and Return a Blob Content and File Name 
	public static List<Object> createConnection(String config, String schema, String table, String blobColName,
			String fileName, String idName, String idVal) throws Exception {

		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new IOException("Failed to read property.json");
		}

		// Access each data source object and retrieve the connectionURL, username, and
		// password
		JsonNode dataSource = rootNode.get(config);
		if (dataSource == null) {
			throw new IOException(config + " Data Source not found");
		}

		connectionURL = dataSource.get("connectionURL").asText();
		username = dataSource.get("username").asText();
		password = dataSource.get("password").asText();

		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		String query = "Select * From " + table + " where " + idName + " = " + idVal;

		List<Object> blobData = new ArrayList<Object>();
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection connection = null;
		ResultSet result = null;
		try {
			String url = connectionURL; // + "/"+ schema;

			connection = DriverManager.getConnection(url, username, password);
			

			// Appending Schema To Connection URL to Access Specific DataBase
			url = url + "/" + schema;

			connection = DriverManager.getConnection(url, username, password);
			log.info("Successfully created connection to : " + connectionURL);

			List<Object> tables = getTables(connection);

			Statement stmt = connection.createStatement();
			result = stmt.executeQuery(query);

			while (result.next()) {
				Object data = result.getObject(blobColName);
				String obtainedFileName = result.getString(fileName);
				blobData.add(data);
				blobData.add(obtainedFileName);
			}
			
		} 

		catch (HiveSQLException e) {
			e.printStackTrace();
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new HiveSQLException(e.getMessage());

		} catch (SQLException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new SQLException(e.getMessage());

		} catch (Exception e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());

		} finally {
			connection.close();
		}
		log.info("Successfully Fetched the BLob Content from table : "+table);
		return blobData;
	}



	// ===============================================================================================================================
	public static ResultSet getResultSet(String config, String schema, String query) throws Exception {

		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
			App.log.error(ArchLensService.writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new IOException("Failed to read property.json");
		}

		// Access each data source object and retrieve the connectionURL, username, and
		// password
		JsonNode dataSource = rootNode.get(config);
		if (dataSource == null) {
			throw new IOException(config + " Data Source not found");
		}

		connectionURL = dataSource.get("connectionURL").asText();
		username = dataSource.get("username").asText();
		password = dataSource.get("password").asText();

		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		log.info("Excecuting query : " + query);

		List<Object> blobData = new ArrayList<Object>();

		Connection connection = null;
		ResultSet result = null;

		String url = connectionURL; // + "/"+ schema;

		connection = DriverManager.getConnection(url, username, password);

		List<Object> schemas = getSchemas(connection);

		// Appending Schema To Connection URL to Access Specific DataBase
		url = url + "/" + schema;
		connection = DriverManager.getConnection(url, username, password);
		log.info("Successfully created connection to : " + url);

		Statement stmt = connection.createStatement();
		result = stmt.executeQuery(query);

		return result;

	}
	
	
//	Method to run a Custom Query
//	public static List<Object> customQuery(String datasource, String schema, String blobColName, String fileName,
//	String query) throws IOException, SQLException, ClassNotFoundException {
//
//String connectionURL = null, username = null, password = null;
//
//// Create an ObjectMapper instance
//ObjectMapper objectMapper = new ObjectMapper();
//
//// Read the JSON file into a JsonNode object
//JsonNode rootNode = null;
//
//try {
//	rootNode = objectMapper.readTree(new File(filePath));
//
//} catch (IOException e) {
//	App.log.error(ArchLensService.writeExceptionInLog(e));
//	App.log.error(e.getMessage());
//	e.printStackTrace();
//	throw new IOException("Failed to read property.json");
//}
//
//// Access each data source object and retrieve the connectionURL, username, and
//// password
//JsonNode dataSource = rootNode.get(datasource);
//if (dataSource == null) {
//	throw new IOException(datasource + " Data Source not found");
//}
//
//connectionURL = dataSource.get("connectionURL").asText();
//username = dataSource.get("username").asText();
//password = dataSource.get("password").asText();
//
//username = ArchLensSecurity.decrypt(username);
//password = ArchLensSecurity.decrypt(password);
//
//System.out.println("Excecuted Query to fetch BLOB content, " + query);
//
//List<Object> blobData = new ArrayList<Object>();
//
//Class.forName("org.apache.hive.jdbc.HiveDriver");
//Connection connection = null;
//ResultSet result = null;
//try {
//	String url = connectionURL; // + "/"+ schema;
//
//	connection = DriverManager.getConnection(url, username, password);
//
//	List<Object> schemas = getSchemas(connection);
//
//	if (!(schemas.contains(schema))) {
//		throw new SQLException(schema + " Schema not found in " + datasource);
//	}
//
//	// Appending Schema To Connection URL to Access Specific DataBase
//	url = url + "/" + schema;
//	connection = DriverManager.getConnection(url, username, password);
//
//	Statement stmt = connection.createStatement();
//	result = stmt.executeQuery(query);
//
//	while (result.next()) {
//		Object data = result.getObject(blobColName);
//		String file_name = result.getString(fileName);
//		blobData.add(data);
//		blobData.add(file_name);
//	}
//} catch (Exception e) {
//	App.log.error(ArchLensService.writeExceptionInLog(e));
//	App.log.error(e.getMessage());
//	e.printStackTrace();
//
//	if (connection == null) {
//		throw new SQLException("Connection Refused : \n" + e.getMessage());
//	} else if (result == null) {
//		throw new SQLException("Failed to query data : \n" + e.getMessage());
//	} else {
//		e.printStackTrace();
//		throw new SQLException("Something went wrong : \n" + e.getMessage());
//	}
//
//} finally {
//	if (connection != null) {
//		connection.close();
//	}
//}
//return blobData;
//}

}
