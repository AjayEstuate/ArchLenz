package com.archlens.controller;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.archlens.configuration.ExternalTableConfig;
import com.archlens.entity.ExternalTableDataSource;
import com.archlens.service.ArchLensService;

@RestController
@CrossOrigin("*")
public class ArchLensController {

	@GetMapping("/")
	public String hello() {
		return "WELCOME TO ARCHLENS";
	}

	@PostMapping("/data-source")
	public ResponseEntity<?> addConfig(@RequestBody ExternalTableDataSource configData) {
		try {
			String respone = ArchLensService.addConfig(configData);
			return new ResponseEntity<String>(respone, HttpStatus.CREATED);
		} catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}

	}

	@GetMapping("/data-sources")
	public List getDataSources() {
		List<String> ds = null;
		try {
			ds = ArchLensService.getJsonKeysFromFile();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<Map<String, String>> mapList = ArchLensService.convertListToMap(ds, "dataSource");
		List list = new ArrayList<>();
		for (Map<String, String> map : mapList) {
			list.add(map);
		}
		return list;

	}

	@GetMapping("/schemas")
	public List getSchemas(String dataSource) {
		List<String> schemas = null;
		try {
			schemas = ExternalTableConfig.getSchemas(dataSource);
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<Map<String, String>> mapList = ArchLensService.convertListToMap(schemas, "schema");
		List list = new ArrayList<>();
		for (Map<String, String> map : mapList) {
			list.add(map);
		}
		return list;
	}

	@GetMapping("/tables")
	public List getTables(String dataSource, String schema) {
		List<String> tables = null;
		try {
			tables = ExternalTableConfig.getTables(dataSource, schema);
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<Map<String, String>> mapList = ArchLensService.convertListToMap(tables, "table");
		List list = new ArrayList<>();
		for (Map<String, String> map : mapList) {
			list.add(map);
		}
		return list;
	}

	@GetMapping(value = "/view", produces = MediaType.ALL_VALUE)
	public ResponseEntity<?> viewBlob(String dataSource, String schema, String table, String blobColName,
			String fileName, String idName, String idVal, HttpServletResponse response) {
		try {
			idVal = URLEncoder.encode(idVal, StandardCharsets.UTF_8.toString());
		} catch (UnsupportedEncodingException e) {

			e.printStackTrace();
		}

		try {
			ArchLensService.viewBlobData(dataSource, schema, table, blobColName, fileName, idName, idVal, response);

			return new ResponseEntity<String>("", HttpStatus.OK);
		} catch (SQLException c) {
			return new ResponseEntity<String>(c.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException p) {
			return new ResponseEntity<String>(p.getMessage(), HttpStatus.BAD_REQUEST);
		}

	}

	@GetMapping(value = "/download", produces = MediaType.ALL_VALUE)
	public ResponseEntity<?> downloadFile(String dataSource, String schema, String table, String blobColName,
			String fileName, String idName, String idVal, HttpServletResponse response) {

		try {
			System.out.println("dataSource"+dataSource);
			String result = ArchLensService.downloadFile(dataSource, schema, table, blobColName, fileName, idName,
					idVal, response);
			return new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (SQLException c) {
			return new ResponseEntity<String>(c.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException p) {
			return new ResponseEntity<String>(p.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}

//	@GetMapping(value = "/custom", produces = MediaType.ALL_VALUE)
//	public ResponseEntity<?> customQuery(String datasource, String schema, String blobColName, String fileName,
//			String query, HttpServletResponse response) {
//		try {
//			System.out.println("API Hitted");
//			String result = ArchLensService.downloadFile(datasource, schema, blobColName, fileName, query, response);
//			return new ResponseEntity<String>(result, HttpStatus.OK);
//		} catch (SQLException c) {
//			return new ResponseEntity<String>(c.getMessage(), HttpStatus.BAD_REQUEST);
//		} catch (IOException p) {
//			return new ResponseEntity<String>(p.getMessage(), HttpStatus.BAD_REQUEST);
//		}
//	}

}
