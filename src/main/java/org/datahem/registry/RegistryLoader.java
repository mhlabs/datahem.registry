package org.datahem.registry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.datahem.registry.DatastoreCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//mvn compile exec:java -Dexec.mainClass="org.datahem.registry.RegistryLoader"
public class RegistryLoader {
	private static final Logger LOG = LoggerFactory.getLogger(RegistryLoader.class);
	
	public static void main(String[] args) {
		DatastoreCache cache = new DatastoreCache();
		RegistryLoader registryLoader = new RegistryLoader();
		try {
			ClassLoader classLoader = registryLoader.getClass().getClassLoader();
			Path configFilePath = Paths.get(classLoader.getResource("schemas").toURI());
			Files.walk(configFilePath)
				.filter(Files::isRegularFile)
				.filter(path -> path.toString().endsWith(".avsc"))
				.forEach(path -> {
					try{
						cache.addSchema(new Schema.Parser().parse(new String(Files.readAllBytes(path), StandardCharsets.UTF_8)));
					}catch(Exception e){
						LOG.error(e.toString());
					}
				});
		} catch(Exception e){
			LOG.error(e.toString());
		}
	}
}