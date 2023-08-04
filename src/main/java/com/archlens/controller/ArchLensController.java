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

import org.apache.hive.service.cli.HiveSQLException;
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
			String response = ArchLensService.addConfig(configData);
			return new ResponseEntity<String>(response, HttpStatus.CREATED);
		} catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.SERVICE_UNAVAILABLE);
		} catch (IllegalArgumentException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}

	@GetMapping("/data-sources")
	public ResponseEntity<?> getDataSources() {
		try {
			List<Map<String, String>> dataSource = ArchLensService.getConnectionUrlsFromJsonFile();
			return new ResponseEntity<List>(dataSource, HttpStatus.CREATED);
		} catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}

	}

	@GetMapping("/schemas")
	public ResponseEntity<?> getSchemas(String dataSource) {
		try {
			List<String> schemas = ExternalTableConfig.getSchemas(dataSource);
			List<Map<String, String>> mapList = ArchLensService.convertListToMap(schemas, "schema");
			List list = new ArrayList<>();
			for (Map<String, String> map : mapList) {
				list.add(map);
			}
			return new ResponseEntity<List>(list, HttpStatus.CREATED);
		} catch (ClassNotFoundException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}

	@GetMapping("/tables")
	public ResponseEntity<?> getTables(String dataSource, String schema) {
		try {
			List<String> tables = null;
			tables = ExternalTableConfig.getTables(dataSource, schema);
			List<Map<String, String>> mapList = ArchLensService.convertListToMap(tables, "table");
			List list = new ArrayList<>();
			for (Map<String, String> map : mapList) {
				list.add(map);
			}
			return new ResponseEntity<List>(list, HttpStatus.CREATED);
		} catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}
	
	
	@GetMapping(value = "/columns")
	public ResponseEntity<?> viewColumn(String dataSource, String schema, String table) {

		try {
			List columns = ArchLensService.viewColumn(dataSource, schema, table);
			
			List<Map<String, String>> mapList = ArchLensService.convertListToMap(columns, "column");
			List list = new ArrayList<>();
			for (Map<String, String> map : mapList) {
				list.add(map);
			}
			
			return new ResponseEntity<List<String>>(list, HttpStatus.OK);
		} catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}

	@GetMapping(value = "/view", produces = MediaType.ALL_VALUE)
	public ResponseEntity<?> viewBlob(String dataSource, String schema, String table, String blobColName,
			String fileName, String idName, String idVal, HttpServletResponse response) {
		try {
			idVal = URLEncoder.encode(idVal, StandardCharsets.UTF_8.toString());
			ArchLensService.viewBlobData(dataSource, schema, table, blobColName, fileName, idName, idVal, response);

			return new ResponseEntity<String>("", HttpStatus.OK);
		} catch (UnsupportedEncodingException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}  catch (HiveSQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}

	}

	@GetMapping(value = "/download", produces = MediaType.ALL_VALUE)
	public ResponseEntity<?> downloadFile(String dataSource, String schema, String table, String blobColName,
			String fileName, String idName, String idVal, HttpServletResponse response) {

		try {
			idVal = URLEncoder.encode(idVal, StandardCharsets.UTF_8.toString());
			String result = ArchLensService.downloadFile(dataSource, schema, table, blobColName, fileName, idName,
					idVal, response);
			return new ResponseEntity<String>(result, HttpStatus.OK);
		} catch (SQLException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (IOException e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			return new ResponseEntity<String>(e.getMessage(), HttpStatus.BAD_REQUEST);
		}
	}

	// @GetMapping(value = "/custom", produces = MediaType.ALL_VALUE)
	// public ResponseEntity<?> customQuery(String datasource, String schema, String
	// blobColName, String fileName,
	// String query, HttpServletResponse response) {
	// try {
	// System.out.println("API Hitted");
	// String result = ArchLensService.downloadFile(datasource, schema, blobColName,
	// fileName, query, response);
	// return new ResponseEntity<String>(result, HttpStatus.OK);
	// } catch (SQLException c) {
	// return new ResponseEntity<String>(c.getMessage(), HttpStatus.BAD_REQUEST);
	// } catch (IOException p) {
	// return new ResponseEntity<String>(p.getMessage(), HttpStatus.BAD_REQUEST);
	// }
	// }


	


}
