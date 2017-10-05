/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ctakes.core.resource;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.apache.uima.resource.DataResource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.SharedResourceObject;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;

import java.io.File;
import java.io.IOException;

/**
 * Oct 2010 - convert to lucene 3.0.2
 * @author Mayo Clinic
 */
public class LuceneIndexReaderResourceImpl
        implements LuceneIndexReaderResource, SharedResourceObject {
	
    // LOG4J logger based on class name
    private Logger log = Logger.getLogger(LuceneIndexReaderResourceImpl.class);

    private IndexReader indexReader = null;

    private final File getLuceneFile(final String path) throws ResourceInitializationException {
    	File file;
	    try {
		    file = FileLocator.getFile(path);
	    } catch (IOException e) {
		    log.error(String.format("Could not find Lucene file index directory for: %s", path));
		    throw new ResourceInitializationException(e);
	    }

	    if (!file.exists()) {
		    log.error(String.format("indexDir=%s does not exist!", path) );
		    throw new ResourceInitializationException(
				    new Exception(String.format("Couldn't find Lucense index folder: %s", path)));
	    }
		// file cannot be null
	    return file;
    }

    private final Directory openLuceneFSDirectory(final File file) throws ResourceInitializationException {
    	try {
			return FSDirectory.open(file);
		} catch (IOException e) {
			log.error(String.format("Could not open Lucene file index directory for: %s", file.getAbsolutePath()));
			throw new ResourceInitializationException(e);
		}
    }
    
    private final Directory openLuceneRAMDirectory(final Directory directory) {
    	log.info( String.format("Loading Lucene index into memory from: %s", directory) );
		try {
			return new RAMDirectory(directory, IOContext.DEFAULT);
		} catch (IOException e) {
			log.warn("Could not load Lucene index as memory directory. Will use original FileSystem lucene index", e);
			return directory;
		}		
    }
    
    private final Directory getLuceneDirectory(final File file, Boolean useMemoryIndex) throws ResourceInitializationException {
    	Directory luceneDirectory = openLuceneFSDirectory(file);
    	if (useMemoryIndex.booleanValue()) {
    		luceneDirectory = openLuceneRAMDirectory(luceneDirectory);
    	}
    	return luceneDirectory;
    }
   
    /**
     * Loads a Lucene index for reading.
     */
    public void load(DataResource dr) throws ResourceInitializationException {

        ConfigurationParameterSettings cps = dr.getMetaData().getConfigurationParameterSettings();
        Boolean useMemoryIndex = (Boolean) cps.getParameterValue("UseMemoryIndex");

        String indexDirStr = (String) cps.getParameterValue("IndexDirectory");

	    Directory directory = getLuceneDirectory( getLuceneFile(indexDirStr), useMemoryIndex );
    	try {
		    // TODO: when upgrade to Lucene 6.0, Directory implements Closeable. Use try-with-resource
    		indexReader = DirectoryReader.open( directory );
			log.info(String.format("Loaded Lucene Index, # docs=%s", indexReader.numDocs()));
		} catch (IOException e) {
			log.error("Cannot traverse Lucene index directory", e);
			indexReader = null;
		}
	}

    public final IndexReader getIndexReader() {
        return indexReader;
    }
}