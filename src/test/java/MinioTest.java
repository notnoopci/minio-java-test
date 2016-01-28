import java.io.*;
import java.net.*;
import java.util.*;

import com.amazonaws.HttpMethod;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.ClientConfiguration;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;


public class MinioTest {
  static AmazonS3Client minioClient() {
    AWSCredentials credentials = new BasicAWSCredentials("1N5LRWZUAYCSPQM6BNIY", "l0pBx0fLD8VY5Oa8po2GLhZgZASTl9yw4FDolvyD");
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setSignerOverride("AWSS3V4SignerType");
    AmazonS3Client s3Client = new AmazonS3Client(credentials, clientConfiguration);
    s3Client.setRegion(Region.getRegion(Regions.US_EAST_1));
    s3Client.setEndpoint("http://localhost:9000");
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true).disableChunkedEncoding());
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation","true");
    
    return s3Client;
  }

  static AmazonS3Client amazonS3Client() {
    AWSCredentials credentials = new BasicAWSCredentials("PLACE_HOLDER_KEY", "PLACE_HOLDER_SECRET");
    AmazonS3Client s3Client = new AmazonS3Client(credentials);
    return s3Client;
  }

  static AmazonS3Client s3Client = minioClient();
  static String TEST_BUCKET = "asdf";

  @BeforeClass
  public static void createBucket() {
    try {
      s3Client.createBucket(TEST_BUCKET);
    } catch (Exception e) { e.printStackTrace(); /* already created hopefully; move on */ }
  }

  PutObjectResult uploadFile(String key, String content) throws Exception {
    InputStream in = new ByteArrayInputStream(content.getBytes("UTF-8"));

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("application/json");
    metadata.addUserMetadata("myKey", "myValue");
    PutObjectRequest request = new PutObjectRequest(TEST_BUCKET, key, in, metadata);

    PutObjectResult result = s3Client.putObject(request);
    return result;
  }

  @Test
  public void simplePutWorks() throws Exception {
    PutObjectResult result = uploadFile("putMetadataTest", "[1, 2]");
    assertTrue(true);
  }

  @Test
  public void getWorks() throws Exception {
    String content = "[3, 2, 1]";
    uploadFile("getWorksTest", content);

    S3Object result = s3Client.getObject(TEST_BUCKET, "getWorksTest");
    BufferedReader br = new BufferedReader(new InputStreamReader(result.getObjectContent()));
    String foundContent = br.lines().parallel().collect(java.util.stream.Collectors.joining("\n"));
    assertEquals(content, foundContent);

    ObjectMetadata metadata = result.getObjectMetadata();
    assertEquals("application/json", metadata.getContentType());
    assertEquals("myValue", metadata.getUserMetaDataOf("myKey"));
  }

  @Test
  public void authenticatedURLWorks() throws Exception {
     String key = "authenticatedURLWorksTest";
     uploadFile(key, "[\"a\"]");

     Date expiry = new java.util.Date(System.currentTimeMillis() + 7 * 60 * 1000);
     URL url = s3Client.generatePresignedUrl(TEST_BUCKET, key, expiry, HttpMethod.GET);
     //assertTrue(url.toString().startsWith("http://localhost:9000/asdf/authenticatedURLWorksTest"));

     HttpURLConnection con = (HttpURLConnection) url.openConnection();

     // optional default is GET
     con.setRequestMethod("GET");

     int responseCode = con.getResponseCode();
     assertEquals(200, responseCode);
  }

  @Test
  public void uploadWithCurlWorks() throws Exception {
     // Based on http://docs.aws.amazon.com/AmazonS3/latest/dev/PresignedUrlUploadObjectJavaSDK.html

     String key = "presignedUploadWorks";
     Date expiry = new java.util.Date(System.currentTimeMillis() + 7 * 60 * 1000);
     URL url = s3Client.generatePresignedUrl(TEST_BUCKET, key, expiry, HttpMethod.PUT);
     //assertTrue(url.toString().startsWith("http://localhost:9000/asdf/presignedUploadWorks"));

     // Upload
     HttpURLConnection connection=(HttpURLConnection) url.openConnection();
     connection.setDoOutput(true);
     connection.setRequestMethod("PUT");
     OutputStreamWriter out = new OutputStreamWriter(
             connection.getOutputStream());
     out.write("This text uploaded as object.");
     out.close();
     int responseCode = connection.getResponseCode();
     assertEquals(200, responseCode);
  }
}
