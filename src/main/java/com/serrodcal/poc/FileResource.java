package com.serrodcal.poc;

import io.quarkus.vertx.web.Param;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import org.jboss.logging.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.vertx.web.Route;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.RoutingContext;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Singleton
public class FileResource {

    private final static String TEMP_DIR = System.getProperty("java.io.tmpdir");

    private static final Logger log = Logger.getLogger(FileResource.class);

    @ConfigProperty(name = "bucket.name")
    String bucketName;

    @Inject
    S3AsyncClient s3;

    @Route(path = "/s3/upload", methods = HttpMethod.POST, consumes = "multipart/form-data")
    public void uploadFile(RoutingContext rc) {
        FileUpload file = rc.fileUploads().iterator().next();

        if (file == null || file.fileName().isEmpty()) {
            rc.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
        }

        Uni.createFrom().completionStage(() -> {
            try {
                return s3.putObject(buildPutRequest(file), AsyncRequestBody.fromFile(uploadToTemp(new FileInputStream(file.uploadedFileName()))));
            } catch (FileNotFoundException ex) {
                log.error(ex.getMessage());
                return CompletableFuture.completedFuture(ex);
            }
        }).subscribe().with(
            result -> rc.response().setStatusCode(HttpResponseStatus.OK.code()).end(),
            failure -> rc.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end()
        );

    }

    @Route(path = "/s3", methods = HttpMethod.GET, produces = "application/json")
    public void listFiles(RoutingContext rc) {
        //Uni<List<FileObject>>
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(bucketName)
                .build();

        Uni.createFrom()
            .completionStage(() -> s3.listObjects(listRequest))
            .onItem().transform(result -> toFileItems(result))
            .subscribe().with(
                result -> rc.response().setStatusCode(HttpResponseStatus.OK.code()).end(Json.encode(result)),
                failure -> rc.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end()
            );
    }

    @Route(path = "/s3/download/:objectKey", methods = HttpMethod.GET, produces = "application/octet-stream")
    public void downloadFile(RoutingContext rc, @Param("objectKey") String objectKey) throws Exception {
        File tempFile = tempFilePath();

        Uni.createFrom()
                .completionStage(() -> s3.getObject(buildGetRequest(objectKey), AsyncResponseTransformer.toFile(tempFile)))
                .subscribe().with(
                    result -> {
                        try {
                            FileInputStream is = new FileInputStream(tempFile);
                            byte[] bytes = new byte[(int)tempFile.length()];
                            is.read(bytes);
                            is.close();

                            rc.response()
                                .setStatusCode(HttpResponseStatus.OK.code())
                                .putHeader("Content-Disposition", "attachment;filename=" + objectKey)
                                .putHeader("Content-Type", result.contentType())
                                .end(Buffer.buffer(bytes));
                        } catch (Exception e) {
                            log.error(e.getMessage());
                            rc.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
                        }
                    },
                    failure -> rc.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end()
                );
    }

    private List<FileObject> toFileItems(ListObjectsResponse objects) {
        return objects.contents().stream()
                .sorted(Comparator.comparing(S3Object::lastModified).reversed())
                .map(FileObject::from).collect(Collectors.toList());
    }

    protected PutObjectRequest buildPutRequest(FileUpload fileUpload) {
        return PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileUpload.fileName())
                .contentType(fileUpload.contentType())
                .build();
    }

    protected GetObjectRequest buildGetRequest(String objectKey) {
        return GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
    }

    protected File tempFilePath() {
        return new File(TEMP_DIR, new StringBuilder().append("s3AsyncDownloadedTemp")
                .append((new Date()).getTime()).append(UUID.randomUUID())
                .append(".").append(".tmp").toString());
    }

    protected File uploadToTemp(InputStream data) {
        File tempPath;
        try {
            tempPath = File.createTempFile("uploadS3Tmp", ".tmp");
            Files.copy(data, tempPath.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return tempPath;
    }

}