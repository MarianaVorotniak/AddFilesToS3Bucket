package example;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AddFiles{

    private static Logger LOGGER = LoggerFactory.getLogger(AddFiles.class);

    private static Regions clientRegion = Regions.EU_WEST_1;
    private static String bucketName = "bucket.of.files";
    private static String folderName = "uploaded/";
    private static Map<String, String> mapOfFilePaths = new HashMap<>();

    private static String directory = "src\\main\\resources\\files\\";

    public static void main(String[] args) {
        try {
            AmazonS3 amazonS3 = initAmazonS3();
            handleRequest(amazonS3);
        }catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    private static AmazonS3 initAmazonS3() {
        LOGGER.debug("Initializing S3 client...");
        return AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .build();
    }

    private static void handleRequest(AmazonS3 s3Client) {
        if (s3Client == null) {
            throw new RuntimeException("S3 Client is null.");
        }

        Map<String, String> mapOfFiles = getListOfFilePaths(directory);

        try {
            LOGGER.debug("Trying to upload files...");
            mapOfFiles.forEach((key, value) -> {
                LOGGER.debug("Uploading file [{}]", key);
                PutObjectRequest request = new PutObjectRequest(bucketName, key, new File(value));
                LOGGER.info("File [{}] uploaded", key);
                s3Client.putObject(request);
            });
        } catch (SdkClientException e) {
            LOGGER.error(e.getMessage());
        }
        LOGGER.info("Files list size [{}]", mapOfFilePaths.size());
        LOGGER.info("All files successfully uploaded to S3 Bucket");
    }

    private static Map<String, String> getListOfFilePaths(String directory) {
        if (directory == null) {
            throw new RuntimeException("File path is null.");
        }

        File folder = new File(directory);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles != null) {
            Arrays.stream(listOfFiles).forEach(file -> {
                if (file.isDirectory()) {
                    getListOfFilePaths(file.getPath());
                } else {
                    mapOfFilePaths.put(folderName + file.getName(), file.getPath());
                }
            });
        }
        return mapOfFilePaths;
    }

}
