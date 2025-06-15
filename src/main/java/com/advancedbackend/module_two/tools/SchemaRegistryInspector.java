package com.advancedbackend.module_two.tools;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class SchemaRegistryInspector {
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args) {
        try {
            System.out.println("=== Schema Registry Information ===\n");

            listSubjects();

            getSchemaVersions("user-topic-value");

            getSchemaDetails("user-topic-value", "1");
            getSchemaDetails("user-topic-value", "latest");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listSubjects() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(SCHEMA_REGISTRY_URL + "/subjects"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("All subjects: " + response.body());
        System.out.println();
    }

    private static void getSchemaVersions(String subject) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(SCHEMA_REGISTRY_URL + "/subjects/" + subject + "/versions"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("Versions for " + subject + ": " + response.body());
        System.out.println();
    }

    private static void getSchemaDetails(String subject, String version) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(SCHEMA_REGISTRY_URL + "/subjects/" + subject + "/versions/" + version))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("Schema details for " + subject + " version " + version + ":");
        System.out.println(response.body());
        System.out.println();
    }
}
