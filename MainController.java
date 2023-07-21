package com.example.Starterproject.controller;

import java.util.Map;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.example.Starterproject.model.Pull;
import com.example.Starterproject.model.Push;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@RestController
@RequestMapping("/users")
public class MainController {
	private Logger logger =LoggerFactory.getLogger(MainController.class);
	
	@Autowired
	private ObjectMapper mapper;
	
	@PostMapping("/upload")
	
	public ResponseEntity<?> pushData(@RequestBody Push pushData)
			
			 
	{
//		if(file.isEmpty()) {
//			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("File can't be empty");
//			
//			
//		}
		this.logger.info("add user request");
	//	logger.info("File Information {}",file.getOriginalFilename());
		logger.info("User:{}",pushData);

		
		
		//converting 
		//Push user =null;
		
//		try {
//			user=mapper.readValue(userData,Push.class);
//			String privateKey=user.getPrivateKey();
//			//System.out.println(privateKey);
//			String secretKey=user.getSecretKey();
//			String bucketName = user.getBucketName();
//			String fileName=user.getFileName();
//			
//			String filePath=user.getFilePath();
			
			//validations
			
			// Process metadata
//			String mandatoryKey = "mandatoryKey";

//			String key = "";
//			String value = "";
			
			
			if (pushData.getMetaData() != null && !pushData.getMetaData().isEmpty()) {
			    // Check if the mandatory key exists
//			    if (pushData.getMetaData().isEmpty()) {
//			        // Handle the missing mandatory key error
//			        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Missing mandatory key");
//			    }
				
//			    if(value == null || value.isEmpty()) {
//					return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Value is required");
//				}
			    
			    // Iterate over metadata key-value pairs
			    for (Map.Entry<String, String> entry : pushData.getMetaData().entrySet()) {
			        String key = entry.getKey();
			        String value = entry.getValue();
			        if(key.isBlank() || key.isEmpty() || key == null) {
			        	return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("key is mandatory");
			        }
			        if(value.isBlank() || value.isEmpty() || value == null) {
			        	return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("value is mandatory");
			        }
			        
//			        // Validate or process the metadata as needed
			        // ...
			        logger.info("Metadat"
			        		+ "a - Key: {}, Value: {}", key, value);
			   
			}
//			else {
//			    // Handle the case when metadata is null or empty
//			    // ...
//			    logger.info("No metadata found.");
//			}
	        
	              
			if(pushData.getPrivateKey() == null || pushData.getPrivateKey().isEmpty()) {
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("private key is mandatory");
			}
			
			if(pushData.getSecretKey() == null || pushData.getSecretKey().isEmpty()) {
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("secret key is mandatory");
			}
			
			if(pushData.getBucketName() == null || pushData.getBucketName().isEmpty()) {
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("bucket name is mandatory");
			}
			
			if(pushData.getFileName() == null || pushData.getFileName().isEmpty()) {
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("file name required");
			}
			
//			if(filePath == null || filePath.isEmpty()) {
//				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
//			}
	        
		}
		
			
			
//		catch(JsonProcessingException e) {
//		
//			e.printStackTrace();
//			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid push request");
//		}
		
		return ResponseEntity.ok("Push request success");
	
	}
	
	@PostMapping("/download")
    public ResponseEntity<?> pullData(@RequestBody Pull pullData) {

//		Pull user =null;
//		
//		try {
//			user=mapper.readValue(userData,Pull.class);
//			String privateKey=user.getPrivateKey();
//			//System.out.println(privateKey);
//			String secretKey=user.getSecretKey();
//			String bucketName = user.getBucketName();
//			String fileName=user.getFileName();
//			String uuid = user.getUuid();
//			
			//validations
		
		if(pullData.getPrivateKey() == null || pullData.getPrivateKey().isEmpty()) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Private key is required");
		}
		
		if(pullData.getSecretKey() == null || pullData.getSecretKey().isEmpty()) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("secret key is required");
		}
		
//        if (pullData.getUuid() == null || pullData.getUuid().isEmpty()) {
//        	
//        	if(pullData.getBucketName() == null || pullData.getBucketName().isEmpty()) {
//    			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("bucket name is required");
//    		}
//    		
//        	if(pullData.getFileName() == null || pullData.getFileName().isEmpty()) {
//    			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("file name is required");
//    		}
//    		
//        	return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("UUID is required");
//        }
//
//        if((pullData.getBucketName() == null || pullData.getBucketName().isEmpty()) 
//        		&& (pullData.getFileName() == null || pullData.getFileName().isEmpty())) {
//        	
//        	if (pullData.getUuid() == null || pullData.getUuid().isEmpty()) {
//        		
//        		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("UUID is required");
//        	}
//        	return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("bucket name and file name both are required");
//        }
		 // Check if either uuid is provided or both bucketName and fileName are provided
        if ((pullData.getUuid() == null || pullData.getUuid().isEmpty())
                && (pullData.getBucketName() == null || pullData.getBucketName().isEmpty()
                || pullData.getFileName() == null || pullData.getFileName().isEmpty())) {

            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Either UUID is required or both bucket name and file name are required");
        }

        // If only one of the following is provided, return an error
//        if ((pullData.getUuid() != null && !pullData.getUuid().isEmpty())
//                && (pullData.getBucketName() != null && !pullData.getBucketName().isEmpty()
//                || pullData.getFileName() != null && !pullData.getFileName().isEmpty())) {
//
//            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
//                    .body("UUID should be provided exclusively or both bucket name and file name should be provided");
//        }

//		}
//		
//		catch(JsonProcessingException e) {
//			
//			e.printStackTrace();
//			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid pull request");
//		}
        return ResponseEntity.ok("Pull request success");
    }
}