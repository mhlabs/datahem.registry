package org.datahem.registry;

/*-
 * ========================LICENSE_START=================================
 * datahem.registry
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =========================LICENSE_END==================================
 */

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
						cache.addSchema(new Schema.Parser().parse(new String(Files.readAllBytes(path), StandardCharsets.UTF_8)), path.toString().substring(path.toString().indexOf("schemas")));
					}catch(Exception e){
						LOG.error(e.toString());
					}
				});
		} catch(Exception e){
			LOG.error(e.toString());
		}
	}
}
