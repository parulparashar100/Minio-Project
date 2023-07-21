package com.example.Starterproject.model;

public class Pull {

	private String secretKey;
	private String privateKey;
	private String bucketName;
	private String fileName;
	//private String filePath; 
	private String uuid;
	public Pull() {
		
	}
	public Pull(String secretKey, String privateKey, String bucketName, String fileName, String uuid) {
		
		this.secretKey = secretKey;
		this.privateKey = privateKey;
		this.bucketName = bucketName;
		this.fileName = fileName;
		this.uuid = uuid;
	}
	public String getSecretKey() {
		return secretKey;
	}
	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
	public String getPrivateKey() {
		return privateKey;
	}
	public void setPrivateKey(String privateKey) {
		this.privateKey = privateKey;
	}
	public String getBucketName() {
		return bucketName;
	}
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	@Override
	public String toString() {
		return "Pull [secretKey=" + secretKey + ", privateKey=" + privateKey + ", bucketName=" + bucketName
				+ ", fileName=" + fileName + ", uuid=" + uuid + "]";
	}
	
	
	
}

