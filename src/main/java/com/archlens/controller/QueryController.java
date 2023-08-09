package com.archlens.controller;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;

import com.archlens.App;
import com.archlens.configuration.ExternalTableConfig;
import com.archlens.service.ArchLensService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
@CrossOrigin("*")
public class QueryController {

	static Logger log = App.log;

	@GetMapping("/data")
	public ResponseEntity<?> query(String dataSource, String schema, String table, String blobColName, String fileName,
			String idName, String query) {
		try {
			log.info("Excecuting query : " + query);

			ResultSet resultSet = ExternalTableConfig.getResultSet(dataSource, schema, query);
			ResultSetMetaData metaData = resultSet.getMetaData();

			List<String> columns = new ArrayList<>();
			List<Map<String, String>> data = new ArrayList<>();

			String blobColumn = null;
			int columnNum = 0;
			int idColumnNum = 0;
			int numColumns = metaData.getColumnCount();

			for (int i = 1; i <= numColumns; i++) {
				String columnName = metaData.getColumnName(i);
				String columnType = metaData.getColumnTypeName(i);

				try {
					if ("binary".equals(columnType)) {
						blobColumn = columnName;
						columnNum = i;
					}
					if (idName.equals(columnName)) {
						idColumnNum = i;
					}
				} catch (NullPointerException e) {
					App.log.error(ArchLensService.writeExceptionInLog(e));
					App.log.error(e.getMessage());
					e.printStackTrace();
				}

				columns.add(columnName);
			}

			while (resultSet.next()) {
				Map<String, String> rowData = new HashMap<>();
				String idVal = null;

				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String columnName = metaData.getColumnName(i);
					String columnValue = resultSet.getString(i);
					try {
						if (i == idColumnNum) {
							idVal = columnValue;
						}
					} catch (NullPointerException e) {
					}

					if (i == columnNum) {
						if (fileName == null) {
							fileName = blobColumn + "_filename";
						}
						if (table != null && idName != null) {
							String viewAPI = "/view?dataSource=" + dataSource + "&schema=" + schema + "&table=" + table
									+ "&blobColName=" + blobColumn + "&fileName=" + fileName + "&idName=" + idName
									+ "&idVal=" + idVal;
							String downloadAPI = "/download?dataSource=" + dataSource + "&schema=" + schema + "&table="
									+ table + "&blobColName=" + blobColumn + "&fileName=" + fileName + "&idName="
									+ idName + "&idVal=" + idVal;

							// "View/Download Links in columnValue ";
							// Replace this with actual view/download links
							columnValue = downloadAPI;
						} else {
							columnValue = "Provide the table name, file name, blob value, and ID name column name to view BLOB data.";
						}
					}

					rowData.put(columnName, columnValue);
				}

				data.add(rowData);
			}

			// Construct the JSON response
			Map<String, Object> jsonResponse = new HashMap<>();
			jsonResponse.put("status", "200 OK");
			jsonResponse.put("data", data);

			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(jsonResponse);

			System.out.println("Query executed successfully. Results are shown");

			return ResponseEntity.ok(json);
		} catch (SQLException | JsonProcessingException e) {
			log.error(ArchLensService.writeExceptionInLog(e));
			log.error(e.getMessage());
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		} catch (Exception e) {
			log.error(ArchLensService.writeExceptionInLog(e));
			log.error(e.getMessage());
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}
}
