package com.archlens.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.hive.service.cli.HiveSQLException;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.archlens.App;
import com.archlens.configuration.ExternalTableConfig;
import com.archlens.entity.ExternalTableDataSource;
import com.archlens.security.ArchLensSecurity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Component
public class ArchLensService {

	// For Local
	// public static String DATA_SOURCE = "src/main/resources/static/property.json";

	// To Deploy
	public static String DATA_SOURCE = "property.json";

//	Method to Convert printStackTrace to String to write printStackTrace in error logs
	public static String writeExceptionInLog(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		String exceptionStackTrace = sw.toString();
		return exceptionStackTrace;
	}

//	Method to Create a properties.json file if not exist in Application Location i
	public static void createJsonFile() throws IOException {
		File file = new File("property.json");
		if (file.createNewFile()) {
			FileWriter writer = new FileWriter(file);
			writer.write("{" + "}");
			writer.close();
			App.log.info("Successfully created Properties.json file.");
		} else {
			App.log.info("Properties.json already exists.");
		}
	}

//	Method to get a list of Data Sources name and connection url 
	public static List<Map<String, String>> getConnectionUrlsFromJsonFile() throws Exception {
		String filePath = ArchLensService.DATA_SOURCE;
		List<Map<String, String>> result = new ArrayList<>();

		try {
			File jsonFile = new File(filePath);
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode rootNode = objectMapper.readTree(jsonFile);

			if (rootNode.isObject()) {
				Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
				while (fields.hasNext()) {
					Map.Entry<String, JsonNode> field = fields.next();
					String key = field.getKey();
					JsonNode dsNode = field.getValue();
					if (dsNode.has("connectionURL")) {
						String connectionURL = dsNode.get("connectionURL").asText();
						Map<String, String> dataSourceInfo = new HashMap<>();
						dataSourceInfo.put("dataSource", key);
						dataSourceInfo.put("connectionURL", connectionURL);
						result.add(dataSourceInfo);
					} else {
						App.log.error(
								"The 'connectionURL' property is missing in properties.json file for key: " + key);
						throw new IOException("The 'connectionURL' property is missing for key: " + key);
					}
				}
			} else {
				App.log.error("The JSON in the file is not an object.");
				throw new IOException("The JSON in the file is not an object.");
			}
		} catch (IOException e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error("Error reading the JSON file : " + e.getMessage());
			throw new Exception("Error reading the JSON file : " + e.getMessage());
		}

		return result;
	}

//	Method to get a Extension from a file name
	public static String getFileExtension(String fileName) {
		if (fileName == null || fileName.isEmpty()) {
			return "";
		}

		int lastDotIndex = fileName.lastIndexOf(".");
		if (lastDotIndex == -1 || lastDotIndex == fileName.length() - 1) {
			// No file extension or dot is the last character
			return "";
		}

		return fileName.substring(lastDotIndex + 1);
	}

//	List of a extension for  Viewable files in browser
	public static List viewableFilesExtension() {
		List<String> fileExtensions = new ArrayList<>();

		fileExtensions.add(".tif");
		fileExtensions.add(".mp3");
		fileExtensions.add(".html");
		fileExtensions.add(".htm");
		fileExtensions.add(".css");
		fileExtensions.add(".js");
		fileExtensions.add(".jpg");
		fileExtensions.add(".jpeg");
		fileExtensions.add(".png");
		fileExtensions.add(".gif");
		fileExtensions.add(".svg");
		fileExtensions.add(".webp");
		fileExtensions.add(".webm");
		fileExtensions.add(".ogg");
		fileExtensions.add(".mp3");
		fileExtensions.add(".wav");
		fileExtensions.add(".pdf");
		fileExtensions.add(".txt");
		fileExtensions.add(".json");
		fileExtensions.add(".xml");
		// fileExtensions.add(".md");
		fileExtensions.add(".csv");
		// fileExtensions.add(".xls");
		// fileExtensions.add(".xlsx");
		fileExtensions.add(".doc");
		fileExtensions.add(".docx");
		// fileExtensions.add(".ppt");
		// fileExtensions.add(".pptx");

		return fileExtensions;
	}

	// Method to convert list to Map
	public static List<Map<String, String>> convertListToMap(List<String> list, String key) {
		List<Map<String, String>> mapList = new ArrayList<>();

		for (String item : list) {
			Map<String, String> map = new HashMap<>();
			map.put(key, item);
			mapList.add(map);
		}
		return mapList;
	}

	/*
	 * Method to Add a Data Source in a Properties.Json file: When we tries to add a
	 * Data source to a properties.json file first it will add to a
	 * property_temp.json if exception occurs it will throw an error else it will
	 * dumb a property_temp.json data to property.json Following this approach
	 * because of following reason: While adding a data to a json file if exception
	 * occurs it will add a { key , , to a json file and throws an error when we try
	 * to add proper data next time to a json file previous special characters and
	 * key will be there that will not allow to maintain a key : value pair
	 */
	public static String addConfig(ExternalTableDataSource configData) throws SQLException, Exception {

		createJsonFile();

		String dataSource = configData.getDataSource();
		String host = configData.getHost();
		String port = configData.getPort();
		String userName = configData.getUserName();
		String password = configData.getPassword();

		String connectionURL = "jdbc:hive2://" + host + ":" + port;
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");

			Connection connection = DriverManager.getConnection(connectionURL, userName, password);

			App.log.info("Successfully created connection to : " + connectionURL);

			userName = ArchLensSecurity.encrypt(userName);
			password = ArchLensSecurity.encrypt(password);

			ObjectMapper objectMapper = new ObjectMapper();
			ObjectNode configNode = objectMapper.createObjectNode();
			ObjectNode connectionNode = objectMapper.createObjectNode();

			connectionNode.put("connectionURL", connectionURL);
			connectionNode.put("username", userName);
			connectionNode.put("password", password);

			// Read existing configurations from the file
			try {
				File configFile = new File(DATA_SOURCE);
				if (configFile.exists()) {
					JsonNode existingConfig = objectMapper.readTree(configFile);
					configNode.setAll((ObjectNode) existingConfig);
				}
			} catch (IOException e) {
				App.log.error(writeExceptionInLog(e));
				App.log.error(e.getMessage());
				e.printStackTrace();
				throw new IOException(e.getMessage());
			}

			// Add the new configuration to the existing ones
			configNode.set(dataSource, connectionNode);

			// Write all configurations to a temporary file
			try {
				File tempFile = new File("property_temp.json");
				FileWriter fileWriter = new FileWriter(tempFile);
				objectMapper.writerWithDefaultPrettyPrinter().writeValue(fileWriter, configNode);
				fileWriter.close();
				App.log.info("DataSource configurations have been updated in property_temp.json.");
			} catch (IOException e) {
				App.log.error(writeExceptionInLog(e));
				App.log.error(e.getMessage());
				App.log.error("Error while writing the configuration to property_temp.json");

				e.printStackTrace();
				throw new IOException("Error while writing the configuration to property_temp.json");
			}

			// Replace the original file with the temporary file
			File tempFile = new File("property_temp.json");
			File configFile = new File(DATA_SOURCE);
			try {
				Files.move(tempFile.toPath(), configFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				App.log.info("DataSource configurations have been added to property.json successfully!");
			} catch (IOException e) {
				App.log.error(writeExceptionInLog(e));
				App.log.error(e.getMessage());
				e.printStackTrace();

				throw new IOException("Failed to add the config.");
			}
			App.log.info(dataSource + " Data source added successfully");
			return dataSource + " Data source added successfully";

		} catch (SQLException e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error(e.getMessage());

			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} catch (IllegalArgumentException e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		} catch (Exception e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

	}

//	If the File Name contains path this function will remove the paths and extract the file name in the path
	public static String getSubstringAfterLastSlash(String filePath) {
		int lastSlashIndex = filePath.lastIndexOf('/');

		if (lastSlashIndex != -1 && lastSlashIndex < filePath.length() - 1) {
			return filePath.substring(lastSlashIndex + 1);
		}

		return "";
	}

	// Method to view Blob Data in browser
	public static void viewBlobData(String datasource, String schema, String table, String blobColName, String fileName,
			String idName, String idVal, HttpServletResponse response) throws Exception {

		List result = ExternalTableConfig.createConnection(datasource, schema, table, blobColName, fileName, idName,
				idVal);

		Object data = result.get(0);
		String obtainedFileName = (String) result.get(1);

		if (obtainedFileName.contains("/")) {
			obtainedFileName = getSubstringAfterLastSlash(obtainedFileName);
		}

		byte[] content = (byte[]) data;

		List suportedExtensions = viewableFilesExtension();

		String extension = "." + getFileExtension(obtainedFileName);
		if (suportedExtensions.contains(extension)) {
			System.out.println("Extension:" + extension + " ,Supported Format");
			viewContent(content, response);
		} else {
			System.out.println("Extension:" + extension + " ,UnSupported Format");
			downloadContent(content, obtainedFileName, response);
		}
	}

	// Method to download a Blob File 
	public static String downloadFile(String datasource, String schema, String table, String blobColName,
			String fileName, String idName, String idVal, HttpServletResponse response)
			throws IOException, HiveSQLException, SQLException, Exception {

		List<Object> result = ExternalTableConfig.createConnection(datasource, schema, table, blobColName, fileName,
				idName, idVal);

		Object fileData = result.get(0);
		String obtainedFileName = (String) result.get(1);

		if (obtainedFileName.contains("/")) {
			obtainedFileName = getSubstringAfterLastSlash(obtainedFileName);
		}

		byte[] content = (byte[]) fileData;

		return downloadContent(content, obtainedFileName, response);

	}
	
// 	Method to read a Binary data and Convert it back to original format and view in browser 
	public static void viewContent(byte[] content, HttpServletResponse response)
			throws FileNotFoundException, IOException {

		String file_name = "Output";
		File file = new File(file_name);
		file.createNewFile();

		try (OutputStream outputStream = new FileOutputStream(file_name)) {
			outputStream.write(content);
		}

		InputStream inputStream = new FileInputStream(file_name);
		response.setContentType(MediaType.ALL_VALUE);
		StreamUtils.copy(inputStream, response.getOutputStream());
		inputStream.close();
		file.delete();

	}

// 	Method to read a Binary data and Convert it back to original format and Download a file 
	public static String downloadContent(byte[] content, String file_name, HttpServletResponse response)
			throws IOException {
		if (content != null) {
			// Set response headers
			response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
			response.setHeader("Content-Disposition", "attachment; filename=\"" + file_name + "\"");

			// Write the content directly to the response output stream
			OutputStream outputStream = response.getOutputStream();
			outputStream.write(content);
			outputStream.flush();
			outputStream.close();
			return file_name + " Downloaded Sucesfully!";
		} else {
			// File not found in the database
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return null;
		}

	}

//	

	public static List viewColumn(String dataSource, String schema, String table) throws Exception {

		String query = "SHOW SCHEMAS";
		String connectionURL = null, username = null, password = null;

		// Create an ObjectMapper instance to read JSON file
		ObjectMapper objectMapper = new ObjectMapper();
		// Read the JSON file into a JsonNode object
		JsonNode rootNode = null;

		try {
			rootNode = objectMapper.readTree(new File(DATA_SOURCE));

		} catch (IOException e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error(e.getMessage());
			throw new IOException("Failed to read property.json");
		}

		JsonNode getDataSource = rootNode.get(dataSource);
		if (getDataSource == null) {
			throw new IOException(getDataSource + " Data Source not found");
		}

		connectionURL = getDataSource.get("connectionURL").asText();
		username = getDataSource.get("username").asText();
		password = getDataSource.get("password").asText();

		// Decrypting UserName and Password
		username = ArchLensSecurity.decrypt(username);
		password = ArchLensSecurity.decrypt(password);

		List<String> columnNames = new ArrayList<String>();
		Set<String> columnNameSet = new HashSet<String>();

		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection connection = DriverManager.getConnection(connectionURL, username, password);
			// Get the metadata for the table
			DatabaseMetaData metaData = connection.getMetaData();

			// Retrieve the columns for the specified table
			try (ResultSet resultSet = metaData.getColumns(null, null, table, null)) {
				while (resultSet.next()) {
					String columnName = resultSet.getString("COLUMN_NAME");
					if (columnNameSet.add(columnName)) {
						columnNames.add(columnName);
					}
				}

			}
			return columnNames;
		} catch (SQLException e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		} catch (Exception e) {
			App.log.error(writeExceptionInLog(e));
			App.log.error(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	
	
	// Custom Query
//		public static String downloadFile(String datasource, String schema, String blobColName, String file_name,
//				String query, HttpServletResponse response) throws IOException, SQLException, ClassNotFoundException {
	//
//			List<Object> result = ExternalTableConfig.customQuery(datasource, schema, blobColName, file_name, query);
//			Object fileData = null;
//			String fileName = null;
//			try {
//				fileData = result.get(0);
//				fileName = (String) result.get(1);
	//
//			} catch (IndexOutOfBoundsException e) {
//				App.log.error(writeExceptionInLog(e));
//				App.log.error(e.getMessage());
//				e.printStackTrace();
	//
//			}
//			if (fileData == null || fileName == null) {
//				throw new SQLException("Failed to Query Data ");
//			} else if (fileName.contains("/")) {
//				fileName = getSubstringAfterLastSlash(fileName);
//			}
	//
//			byte[] content = (byte[]) fileData;
//			// Set response headers
//			response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
//			response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
	//
//			// Write the content directly to the response output stream
//			OutputStream outputStream = response.getOutputStream();
//			outputStream.write(content);
//			outputStream.flush();
//			outputStream.close();
//			return fileName + " Downloaded Sucesfully!";
	//
//		}

}
