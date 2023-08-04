package com.archlens;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.archlens.service.ArchLensService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Test {


	public static List<String> getJsonKeysFromFile() throws Exception {
		//		String filePath = ArchLensService.DATA_SOURCE;
		List<String> keys = new ArrayList<>();

		try {
			File jsonFile = new File(DATA_SOURCE);
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode rootNode = objectMapper.readTree(jsonFile);

			if (rootNode.isObject()) {
				Iterator<String> fieldNames = rootNode.fieldNames();
				while (fieldNames.hasNext()) {
					String key = fieldNames.next();
					keys.add(key);
				}
				System.out.println("Readed Json File ");
			} else {
				System.out.println("The JSON in the file is not an object.");
			}
		} catch (IOException e) {
			System.out.println("Error reading the JSON file: " + e.getMessage());
			throw new Exception("Error reading the JSON file : " + e.getMessage());
		}

		return keys;
	}

	public static String getFilePath(String fileName)  {
		// Create a ClassPathResource for the given file name.
		Resource resource = new ClassPathResource("static/" + fileName);

		// Get the actual file path from the resource.
		String filePath = null;
		try {
			filePath = resource.getFile().getAbsolutePath();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return filePath;
	}
	//	public  static String DATA_SOURCE = "C:\\Users\\ADevaraju\\Desktop\\ArchLens\\src\\main\\resources\\static\\property.json";
	//	public  static String DATA_SOURCE = System.getProperty("user.dir")+ "/Archlens/src/main/resources/static/property.json";

	//	public  static String DATA_SOURCE = "src/main/resources/static/property.json1";
	public  static String DATA_SOURCE = getFilePath("property.json");

	public static void main(String[] args) {
		try {
//			System.out.println(getConnectionUrlsFromJsonFile());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		try {
//			System.out.println("System.getProperty(\"user.dir\")--->"+System.getProperty("user.dir"));
//			System.out.println(DATA_SOURCE);
//			System.out.println(getJsonKeysFromFile());
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	



}
