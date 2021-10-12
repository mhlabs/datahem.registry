package org.datahem.registry;

// [START functions_helloworld_http]

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

public class HelloHttp implements HttpFunction {
  private static final Logger logger = Logger.getLogger(HelloHttp.class.getName());

  private static final Gson gson = new Gson();

  @Override
  public void service(HttpRequest request, HttpResponse response)
      throws IOException {
    // Check URL parameters for "name" field
    // "world" is the default value
    String name = request.getFirstQueryParameter("name").orElse("world");

    // Parse JSON request and check for "name" field
    try {
      Pattern pattern = Pattern.compile("/subjects/(.*)/versions/(.*)");
      Matcher matcher = pattern.matcher(request.getPath());
      if(matcher.matches()){
        SchemaVersionNumber svn = SchemaVersionNumber.builder().latestVersion(true).build();
        String subject = matcher.group(1);
        String version = matcher.group(2);
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
        GetSchemaResponse gsr = gc.getSchema(GetSchemaRequest.builder()
            .schemaId(SchemaId.builder()
                .schemaName(subject)
                .registryName("PocSchemaRegistry")
                //.schemaArn("arn:aws:glue:eu-west-1:751354400372:schema/PocSchemaRegistry/TestSchema")
                .build()
            ).build());
            logger.info(gsr.toString());
            GetSchemaVersionResponse gsvr = gc.getSchemaVersion(GetSchemaVersionRequest.builder()
            .schemaId(SchemaId.builder()
                .schemaArn("arn:aws:glue:eu-west-1:751354400372:schema/PocSchemaRegistry/TestSchema")
                .build()
            )
            .schemaVersionNumber(svn)
            .build());
        logger.info(gsvr.toString());
        name = gsvr.schemaDefinition();
      }
    } catch (Exception e) {
      logger.severe("Error: " + e.getMessage());
    }

    var writer = new PrintWriter(response.getWriter());
    writer.printf("SchemaDefiniton: %s", name);
  }
}
// [END functions_helloworld_http]