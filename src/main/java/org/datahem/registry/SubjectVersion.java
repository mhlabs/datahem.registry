package org.datahem.registry;

// [START functions_subjectVersion_http]

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

public class SubjectVersion implements HttpFunction {
  private static final Logger logger = Logger.getLogger(SubjectVersion.class.getName());

  @Override
  public void service(HttpRequest request, HttpResponse response) throws IOException {
    JsonObject responsePayload =  new JsonObject();
    try {
      Pattern pattern = Pattern.compile("/subjects/(.*)/versions/(.*)");
      Matcher matcher = pattern.matcher(request.getPath());
      
      if(matcher.matches()){
        String subject = matcher.group(1);
        String version = matcher.group(2);
        SchemaVersionNumber svn = SchemaVersionNumber.builder()
          .latestVersion(true)
          .build();
        
        // check if version is latest or a version number
        if(!version.equals("latest")){
          try {
            long l = Long.parseLong(version);
            svn = SchemaVersionNumber.builder().versionNumber(l).build();
          } catch (NumberFormatException nfe) {
            logger.severe("Error parsing Long: " + nfe.getMessage());
          }
        }

        GlueClient gc = GlueClient.builder()
            .httpClient(ApacheHttpClient.builder().build())
            .build();

        GetSchemaVersionResponse gsvr = gc.getSchemaVersion(GetSchemaVersionRequest.builder()
          .schemaId(SchemaId.builder()
            .schemaName(subject)
            .registryName("PocSchemaRegistry")
              .build()
          )
          .schemaVersionNumber(svn)
          .build());

        responsePayload.addProperty("subject", subject);
        responsePayload.addProperty("id", gsvr.schemaVersionId());
        responsePayload.addProperty("version", gsvr.versionNumber());
        responsePayload.addProperty("schemaType", gsvr.dataFormatAsString());
        responsePayload.addProperty("schema", gsvr.schemaDefinition());
      }
    } catch (Exception e) {
      logger.severe("Error: " + e.getMessage());
    }

    response.setContentType("application/json; charset=utf-8");
    var writer = new PrintWriter(response.getWriter());
    writer.printf(responsePayload.toString());
  }
}
// [END functions_subjectVersion_http]