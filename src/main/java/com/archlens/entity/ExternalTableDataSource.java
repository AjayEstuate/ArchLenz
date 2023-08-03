package com.archlens.entity;

public class ExternalTableDataSource {
	
	private String dataSource;
	private String host;
	private String port;
	private String userName;
	private String  password;
	
	
	
	public ExternalTableDataSource(String dataSource, String host, String port, String userName, String password) {
		super();
		this.dataSource = dataSource;
		this.host = host;
		this.port = port;
		this.userName = userName;
		this.password = password;
	}
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	

}
