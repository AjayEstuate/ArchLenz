package com.archlens;

import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import com.archlens.controller.ArchLensController;

@SpringBootApplication
public class App extends SpringBootServletInitializer {
	
	public static Logger log = org.slf4j.LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		

		SpringApplication.run(App.class, args);
		
	}

}
