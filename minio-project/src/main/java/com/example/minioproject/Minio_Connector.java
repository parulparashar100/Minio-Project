package com.example.minioproject;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveBucketArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;

public class Minio_Connector {

	public static String server_url = "http://10.101.104.140:9000";
	public static String secret_key = "minioadmin";
	public static String access_key = "minioadmin";

	public MinioClient client;

	public Minio_Connector() {
		try {
			client = MinioClient.builder().endpoint(server_url)
								.credentials(access_key, secret_key)
								.build();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void bucketList() {
		try {
			List<Bucket> list = client.listBuckets();
			for (Bucket b : list) {
				System.out.println(b.name());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void objectList(String bucketName) {
		try {

			Iterable<Result<Item>> items = client.listObjects(
					ListObjectsArgs.builder().bucket(bucketName).build());
			for (Result<Item> i : items) {
				System.out.println(i.get().objectName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createBucket(String bucketName) {
		try {
			client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
			System.out.println("Bucket created successfully");
		} catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
				| InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
				| IllegalArgumentException | IOException e) {
			e.printStackTrace();
			System.out.println("Unable to create bucket");
		}
	}

	public void deleteBucket(String bucketName) {
		try {
			client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
			System.out.println(bucketName + " bucket deleted successfully");
		} catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
				| InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
				| IllegalArgumentException | IOException e) {
			e.printStackTrace();
			System.out.println("Unable to deleted bucket");
		}
	}

	public void pushData(String bucketName, String objectName, String filePath) {
		try {
			boolean found = client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
			if (found) {
				System.out.println(bucketName + " bucket exist");
				FileInputStream fin = new FileInputStream(filePath);
				ObjectWriteResponse response = client.putObject(PutObjectArgs.builder().bucket(bucketName)
						.object(objectName).stream(fin, -1, 10485760).build());
				System.out.println(response.object());
			} else {
				System.out.println(bucketName + " bucket does not exist!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void pullObject(String bucketName, String objectName, String fileName) {
		try {
			client.downloadObject(DownloadObjectArgs.builder()
										.bucket(bucketName)
										.object(objectName)
										.filename(fileName)
										.build());
			System.out.println("Object pulled successfully");
		} catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
				| InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
				| IllegalArgumentException | IOException e) {
			e.printStackTrace();
			System.out.println("Unable to pull object");
		}
	}

	public void deleteObject(String bucketName, String objectName) {
		try {
			client.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
			System.out.println(objectName + " deleted successfully");
		} catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
				| InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
				| IllegalArgumentException | IOException e) {
			e.printStackTrace();
			System.out.println("Unable to delete object");
		}
	}

	public static void main(String args[]) {
		try {
			Minio_Connector minio = new Minio_Connector();
//			minio.bucketList();
//			minio.objectList("amisha");
//			minio.deleteObject("parul", "testobject");
//			minio.deleteBucket("parul");
//			minio.createBucket("parul");
//			minio.pushData("parul", "testobject", "D:\\eclipse workspace\\minio\\Minio Testdata.txt");
			minio.pullObject("parul", "testobject", "D:\\eclipse workspace\\minio\\pulled data.txt");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
