package com.xxss.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Random;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;

public class AmazonS3UploadFile {
	private static S3Client s3;
	private static final Region region = Region.US_WEST_2;

	/**
	 * Uploading an object to S3 in parts
	 */
	private static void multipartUpload(File file, String bucketName) throws IOException {
		long size = file.length();
		s3 = S3Client.builder().region(region).build();
		// First create a multipart upload and get upload id
		CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
				.bucket(bucketName).key(file.getName()).build();
		CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
		String uploadId = response.uploadId();
		System.out.println(uploadId);

		UploadPartRequest uploadPartRequest1 = UploadPartRequest.builder().bucket(bucketName).key(file.getName())
				.uploadId(uploadId).partNumber(1).build();
		String etag1 = s3
				.uploadPart(uploadPartRequest1, RequestBody.fromInputStream(new FileInputStream(file), size / 2))
				.eTag();
		CompletedPart part1 = CompletedPart.builder().partNumber(1).eTag(etag1).build();

		UploadPartRequest uploadPartRequest2 = UploadPartRequest.builder().bucket(bucketName).key(file.getName())
				.uploadId(uploadId).partNumber(2).build();
		String etag2 = s3
				.uploadPart(uploadPartRequest2, RequestBody.fromInputStream(new FileInputStream(file), size / 2))
				.eTag();
		CompletedPart part2 = CompletedPart.builder().partNumber(2).eTag(etag2).build();

		// Finally call completeMultipartUpload operation to tell S3 to merge all
		// uploaded
		// parts and finish the multipart operation.
		CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(part1, part2)
				.build();
		CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
				.bucket(bucketName).key(file.getName()).uploadId(uploadId).multipartUpload(completedMultipartUpload)
				.build();
		s3.completeMultipartUpload(completeMultipartUploadRequest);
	}

	public static void main(String[] args) throws IOException {
		multipartUpload(new File("F:\\videos\\91KK哥（换新ID阿飞Alfie）12-阿飞正传第一部-飞雪连天射174黑丝空姐（完整版）"), "liuyichong2");

	}

	

}
