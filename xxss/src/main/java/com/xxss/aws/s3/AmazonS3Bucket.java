package com.xxss.aws.s3;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AmazonS3Bucket {

	public static final Region region = Region.US_WEST_2;

	/**
	 * 关闭AmazonS3 客户端
	 * 
	 * @param s3
	 */
	public static void close(S3Client s3) {
		try {
			s3.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 获取所有的存储桶的名字.
	 * 
	 * @return 返回 list
	 */
	public static List<String> getAllBuckets() {
		List<String> list = new ArrayList<String>();
		S3Client s3 = S3Client.builder().region(region).build();
		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
		listBucketsResponse.buckets().stream().forEach(x -> list.add(x.name()));
		for (String string : list) {
			System.out.println(string);
		}
		close(s3);
		return list;
	}

	/**
	 * 创建一个存储通
	 * 
	 * @param bucktName
	 *            存储桶名字
	 */
	public static void creatBucket(String bucktName) {
		S3Client s3 = S3Client.builder().region(region).build();
		CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(bucktName)
				.createBucketConfiguration(
						CreateBucketConfiguration.builder().locationConstraint(region.value()).build())
				.build();
		s3.createBucket(createBucketRequest);
		close(s3);
	}

	/**
	 * 删除一个空的BUCKET
	 * 
	 * @param bucketName
	 *            存储桶名字
	 */
	public static void deleteEmptyBucket(String bucket) {
		S3Client s3 = S3Client.builder().region(region).build();
		DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
		s3.deleteBucket(deleteBucketRequest);
		close(s3);
	}

	/**
	 * 检索存储桶中的所有对象
	 * 
	 * @param bucket
	 *            存储桶名字
	 * @return
	 */
	public static List<S3Object> listObjectsV2(String bucket) {
		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
		ListObjectsV2Response listObjectsV2Response;
		S3Client s3 = S3Client.builder().region(region).build();
		listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
		return listObjectsV2Response.contents();
	}

	/**
	 * 删除桶中的一个对象
	 * 
	 * @param s3object
	 *            对象
	 * @param bucket
	 *            存储桶名字
	 */
	public static void deleteS3Object(S3Object s3object, String bucket) {
		S3Client s3 = S3Client.builder().region(region).build();
		s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(s3object.key()).build());
	}

	public static void main(String[] args) {
		listObjectsV2("liuyichong2");
	}
}
