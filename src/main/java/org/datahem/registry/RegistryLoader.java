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

//mvn compile exec:java -Dexec.mainClass="org.datahem.registry.RegistryLoader"
public class RegistryLoader {
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
						e.printStackTrace();
					}
				});
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}