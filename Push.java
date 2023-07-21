package com.example.Starterproject.model;

import java.util.Map;


public class Push {
	
	private String secretKey;
	private String privateKey;
	private String bucketName;
	private String fileName;
//	private String filePath; 
	private Map<String, String> metaData;
	
	
	public Push() {
		
		
	}

	public Push(String secretKey, String privateKey, String bucketName, String fileName, Map<String, String> metaData) {
        this.secretKey = secretKey;
        this.privateKey = privateKey;
        this.bucketName = bucketName;
        this.fileName = fileName;
//        this.filePath = filePath;
        this.metaData = metaData;
    }

	@Override
    public String toString() {
        return "Push [secretKey=" + secretKey + ", privateKey=" + privateKey + ", bucketName=" + bucketName
                + ", fileName=" + fileName + ", filePath=" + ", metaData=" + metaData + "]";
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


//	public String getFilePath() {
//		return filePath;
//	}
//
//
//	public void setFilePath(String filePath) {
//		this.filePath = filePath;
//	}

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }
    
    public Map<String, String> getMetaData() {
    	return metaData;
    }


	

}
