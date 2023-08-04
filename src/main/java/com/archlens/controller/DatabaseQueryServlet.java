package com.archlens.controller;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.servlet.http.HttpServlet;

import org.apache.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;

import com.archlens.configuration.ExternalTableConfig;

@Controller
@CrossOrigin("*")
public class DatabaseQueryServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@GetMapping("/query")
	public ResponseEntity<?> query(String dataSource, String schema, String table, String blobColName, String fileName,
			String idName, String query) {
		try {
			System.out.println("Processing query " + query);

			ResultSet resultSet = ExternalTableConfig.getResultSet(dataSource, schema, query);
			ResultSetMetaData metaData = resultSet.getMetaData();

			StringBuilder htmlOutput = new StringBuilder();
			htmlOutput.append("<html>");
			htmlOutput.append("<head><title>ArchLens</title></head>");
			htmlOutput.append("<body>");
			htmlOutput.append("<h2>").append("Fetched").append(" Results:</h2>");

			htmlOutput.append("<table border=\"1\">");
			htmlOutput.append("<tr>");
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				htmlOutput.append("<th>").append(metaData.getColumnName(i)).append("</th>");
			}
			htmlOutput.append("</tr>");

			String blobColumn = null;

			// To get Blob Column name and Column Number
			int columnNum = 0;
			int idColumnNum = 0;
			int numColumns = metaData.getColumnCount();

			for (int i = 1; i <= numColumns; i++) {
				String columnName = metaData.getColumnName(i);
				String columnType = metaData.getColumnTypeName(i);

				// Assaigning Blob Column name and Column Number

				try {
					if ("binary".equals(columnType)) {
						blobColumn = columnName;
						columnNum = i;

					}
					if (idName.equals(columnName)) {
						idColumnNum = i;
					}
				} catch (NullPointerException e) {

				}

			}

			while (resultSet.next()) {
				htmlOutput.append("<tr>");
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
							String viewAPI = "/ArchLenz/view?dataSource=" + dataSource + "&schema=" + schema + "&table=" + table
									+ "&blobColName=" + blobColumn + "&fileName=" + fileName + "&idName=" + idName
									+ "&idVal=" + idVal;
							String downloadAPI = "/ArchLenz/download?dataSource=" + dataSource + "&schema=" + schema + "&table="
									+ table + "&blobColName=" + blobColumn + "&fileName=" + fileName + "&idName="
									+ idName + "&idVal=" + idVal;

							String view = "<a href=\"" + viewAPI + "\" target=\"_blank\">View</a>";
							String download = "<a href=\"" + downloadAPI + "\" target=\"_blank\">Download</a>";

							columnValue = view + "    " + download;
						} else {
							columnValue = "Provide the table name, file name , blob value and ID name column name to view BLOB data.";
						}
					}

					htmlOutput.append("<td>").append(columnValue).append("</td>");
				}

				htmlOutput.append("</tr>");
			}

			htmlOutput.append("</table>");
			htmlOutput.append("</body></html>");

			System.out.println("Query executed successfully. Results are shown");

			return ResponseEntity.ok(htmlOutput.toString());

		} catch (SQLException e) {
			return ResponseEntity.status(HttpStatus.SC_NOT_FOUND).body(e.getMessage());
		} catch (IOException e) {
			return ResponseEntity.status(HttpStatus.SC_NOT_FOUND).body(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}
}
