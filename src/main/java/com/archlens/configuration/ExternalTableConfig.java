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

import com.archlens.security.ArchLensSecurity;
import com.archlens.service.ArchLensService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


//@Configuration
public class ExternalTableConfig {

	static String filePath = ArchLensService.DATA_SOURCE;

	public static List<Object> getSchemas(Connection connection) throws SQLException {

		String query = "SHOW SCHEMAS";

		try {
			Statement stmt = connection.createStatement();
			ResultSet resultSet = stmt.executeQuery(query);
			ResultSetMetaData metadata = resultSet.getMetaData();
			int columnCount = metadata.getColumnCount();

			List schemas = new ArrayList<Object>();
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					Object value = resultSet.getObject(i);
					schemas.add(value);
				}
			}
			return schemas;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<String> getSchemas(String config) throws SQLException, IOException {

		String query = "SHOW SCHEMAS";
		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance to read JSON file
		ObjectMapper objectMapper = new ObjectMapper();
		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
			throw new IOException("Failed to read property.json");
		}

		JsonNode dataSource = rootNode.get(config);
		if (dataSource == null) {
			throw new IOException(config + " Data Source not found");
		}

		connectionURL = dataSource.get("connectionURL").asText();
		username = dataSource.get("username").asText();
		password = dataSource.get("password").asText();

		// Decrypting UserName and Password
		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		try {
			Connection connection = DriverManager.getConnection(connectionURL, username, password);
			Statement stmt = connection.createStatement();
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
		} catch (SQLException s) {
			s.printStackTrace();
			throw new  SQLException(s.getMessage());
		} 
	}

	public static List<Object> getTables(Connection connection) throws SQLException {
		String query = "SHOW Tables";
		List<Object> tables = new ArrayList<Object>();
		try {
			Statement stmt = connection.createStatement();
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

		} catch (Exception e) {
			e.printStackTrace();
		}
		return tables;
	}

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
			Statement stmt = connection.createStatement();
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

		} catch (SQLException s) {
			s.printStackTrace();
			throw new  SQLException(s.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new  Exception(e.getMessage());
		}

	}

	public static List<Object> createConnection(String config, String schema, String table, String blobColName,
			String fileName, String idName, String idVal) throws  Exception {

		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
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

		System.out.println("Excecuted Query to fetch BLOB content, " + query);

		List<Object> blobData = new ArrayList<Object>();

		Connection connection = null;
		ResultSet result = null;
		try {
			String url = connectionURL; // + "/"+ schema;

			connection = DriverManager.getConnection(url, username, password);

//			List<Object> schemas = getSchemas(connection);
//
//			if (!(schemas.contains(schema))) {
//				throw new SQLException("Schema not found");
//			}

			// Appending Schema To Connection URL to Access Specific DataBase
			url = url + "/" + schema;
			connection = DriverManager.getConnection(url, username, password);

			List<Object> tables = getTables(connection);

//			if (!(tables.contains(table))) {
//				throw new SQLException("Table not found");
//			} else {
//
//			}

			Statement stmt = connection.createStatement();
			result = stmt.executeQuery(query);

			while (result.next()) {
				Object data = result.getObject(blobColName);
				String obtainedFileName = result.getString(fileName);
				blobData.add(data);
				blobData.add(obtainedFileName);
			}
		} 
//		catch (AnalysisException   e) {
//			e.printStackTrace();
//			throw new AnalysisException( e.getMessage(), null);
//
//		}
		catch (HiveSQLException   e) {
			e.printStackTrace();
			throw new HiveSQLException( e.getMessage());
//			throw new AnalysisException( e.getMessage(), null);

		}catch (SQLException   e) {
			e.printStackTrace();
			throw new SQLException( e.getMessage());

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception( e.getMessage());

		}  finally {
			connection.close();
		}
		return blobData;
	}



	public static List<Object> customQuery(String datasource, String schema, String blobColName, String fileName,
			String query) throws IOException, SQLException {

		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
			throw new IOException("Failed to read property.json");
		}

		// Access each data source object and retrieve the connectionURL, username, and
		// password
		JsonNode dataSource = rootNode.get(datasource);
		if (dataSource == null) {
			throw new IOException(datasource + " Data Source not found");
		}

		connectionURL = dataSource.get("connectionURL").asText();
		username = dataSource.get("username").asText();
		password = dataSource.get("password").asText();

		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		System.out.println("Excecuted Query to fetch BLOB content, " + query);

		List<Object> blobData = new ArrayList<Object>();

		Connection connection = null;
		ResultSet result = null;
		try {
			String url = connectionURL; // + "/"+ schema;

			connection = DriverManager.getConnection(url, username, password);

			List<Object> schemas = getSchemas(connection);

			if (!(schemas.contains(schema))) {
				throw new SQLException(schema + " Schema not found in " + datasource);
			}

			// Appending Schema To Connection URL to Access Specific DataBase
			url = url + "/" + schema;
			connection = DriverManager.getConnection(url, username, password);

			Statement stmt = connection.createStatement();
			result = stmt.executeQuery(query);

			while (result.next()) {
				Object data = result.getObject(blobColName);
				String file_name = result.getString(fileName);
				blobData.add(data);
				blobData.add(file_name);
			}
		} catch (Exception e) {
			e.printStackTrace();

			if (connection == null) {
				throw new SQLException("Connection Refused : \n" + e.getMessage());
			} else if (result == null) {
				throw new SQLException("Failed to query data : \n" + e.getMessage());
			} else {
				e.printStackTrace();
				throw new SQLException("Something went wrong : \n" + e.getMessage());
			}

		} finally {
			if (connection != null) {
				connection.close();
			}
		}
		return blobData;
	}

	// ===============================================================================================================================
	public static ResultSet getResultSet(String config, String schema, String query) throws IOException, SQLException {

		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance
		ObjectMapper objectMapper = new ObjectMapper();

		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(filePath));

		} catch (IOException e) {
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

		System.out.println("Excecuted Query to fetch BLOB content, " + query);

		List<Object> blobData = new ArrayList<Object>();

		Connection connection = null;
		ResultSet result = null;

		String url = connectionURL; // + "/"+ schema;

		connection = DriverManager.getConnection(url, username, password);

		List<Object> schemas = getSchemas(connection);

//		if (!(schemas.contains(schema))) {
//			throw new SQLException("Schema not found");
//		}
		// Appending Schema To Connection URL to Access Specific DataBase
		url = url + "/" + schema;
		connection = DriverManager.getConnection(url, username, password);

		Statement stmt = connection.createStatement();
		result = stmt.executeQuery(query);

		return result;

	}

}
