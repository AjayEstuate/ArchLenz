//package com.archlens.controller;
//
//import java.io.IOException;
//import java.io.OutputStream;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.List;
//import javax.servlet.http.HttpServletResponse;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.MediaType;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.GetMapping;
//import org.springframework.web.bind.annotation.PathVariable;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestBody;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;
//
//import com.archlens.configuration.ExternalTableConfig;
//import com.archlens.entity.ExternalTableDataSource;
//import com.archlens.service.ArchLensService;
//
//@RestController
//@RequestMapping("/ArchLens")
//public class ArchLensController {
//
//
//	@GetMapping("/")
//	public String home(){
//		return "Welcome to ArchLens";
//	}
//	
//
//	@PostMapping("/data-source")
//	public ResponseEntity<?> addConfig(@RequestBody ExternalTableDataSource configData) {
//		try {
//			String respone =  ArchLensService.addConfig(configData);
//			return new ResponseEntity<String>(respone, HttpStatus.CREATED);
//		} catch (SQLException e) {
//			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
//		}  catch (Exception e) {
//			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
//		} 
//
//	}
//	
//	@GetMapping("/data-sources")
//	public List getDataSources() {
//		List schemas = null;
//		try {
//			schemas = ArchLensService.getJsonKeysFromFile();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return schemas;
//	}
//
//	@GetMapping("/ds={datasource}/schemas")
//	public List getSchemas(@PathVariable String datasource) {
//		List schemas = null;
//		try {
//			schemas = ExternalTableConfig.getSchemas(datasource);
//		} catch (SQLException | IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return schemas;
//	}
//
//	@GetMapping("/ds={datasource}/schema={schema}/tables")
//	public List getTables(@PathVariable String datasource, @PathVariable String schema) {
//		List schemas = null;
//		try {
//			schemas = ExternalTableConfig.getSchemas(datasource);
//		} catch (SQLException | IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return schemas;
//	}
//
//	@GetMapping(value = "/view/ds={datasource}/schema={schema}/tablename={table}/blobColName={blobColName}/fileName={fileName}/{idName}={idVal}", produces = MediaType.ALL_VALUE)
//	public ResponseEntity<?> viewBlob(@PathVariable String datasource, @PathVariable String schema,
//			@PathVariable String table, @PathVariable String blobColName, @PathVariable String fileName,
//			@PathVariable String idName, @PathVariable String idVal, HttpServletResponse response) {
//
//		try {
//			ArchLensService.viewBlobData(datasource, schema, table, blobColName, idName, idVal, idVal, response);
//
//			return new ResponseEntity<String>("Details Fetched Sucesfully", HttpStatus.CREATED);
//		} catch (SQLException c) {
//			return new ResponseEntity<String>(c.getMessage(), HttpStatus.BAD_REQUEST);
//		} catch (IOException p) {
//			return new ResponseEntity<String>(p.getMessage(), HttpStatus.BAD_REQUEST);
//		}
//
//	}
//
//
//	@GetMapping(value = "/download/ds={datasource}/schema={schema}/tablename={table}/blobColName={blobColName}/fileName={file_name}/{idName}={idVal}", produces = MediaType.ALL_VALUE)
//	public void downloadFile(@PathVariable String datasource, @PathVariable String schema, @PathVariable String table,
//			@PathVariable String blobColName, @PathVariable String file_name, @PathVariable String idName,
//			@PathVariable String idVal, HttpServletResponse response) throws IOException, SQLException {
//
//		List result = ExternalTableConfig.createConnection(datasource, schema, table, blobColName, file_name, idName,idVal);
//
//		Object fileData = result.get(0);
//		String fileName = (String) result.get(1);
//
//		byte[] content = (byte[]) fileData;
//		if (fileData != null) {
//			// Set response headers
//			response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
//			response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
//
//			// Write the content directly to the response output stream
//			OutputStream outputStream = response.getOutputStream();
//			outputStream.write(content);
//			outputStream.flush();
//			outputStream.close();
//		} else {
//			// File not found in the database
//			response.sendError(HttpServletResponse.SC_NOT_FOUND);
//		}
//
//	}
//
//
//
//
//}



