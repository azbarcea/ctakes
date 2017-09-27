/*
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
package org.apache.ctakes.core.ae;

import org.apache.ctakes.typesystem.type.textspan.Segment;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.fit.pipeline.JCasIterable;
import org.apache.uima.fit.pipeline.JCasIterator;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.cleartk.util.cr.FilesCollectionReader;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

public class TestCDASegmentAnnotator {

	public static String INPUT_FILE = "../ctakes-regression-test/testdata/input/plaintext/doc2_07543210_sample_current.txt";

	static Logger LOGGER = Logger.getLogger(TestCDASegmentAnnotator.class);

	public static final void printSegments(JCas jCas) {
		for (Segment segment : JCasUtil.select(jCas, Segment.class))
			LOGGER.info(String.format("Segment:%s\tBegin:%d\tEnd:%d\t%s",
					segment.getId(), segment.getBegin(), segment.getEnd(), segment.getPreferredText()));
	}

	@Test
	public void TestCDASegmentPipeLine() throws ResourceInitializationException {
		TypeSystemDescription typeSystem = TypeSystemDescriptionFactory
				.createTypeSystemDescription();

		CollectionReaderDescription reader = CollectionReaderFactory.createReaderDescription(
				FilesCollectionReader.class,
				typeSystem,
				FilesCollectionReader.PARAM_ROOT_FILE,
				INPUT_FILE);

		AnalysisEngineDescription sectionAnnotator = AnalysisEngineFactory
				.createEngineDescription(CDASegmentAnnotator.class, typeSystem);
		AnalysisEngineDescription dumpOutput = AnalysisEngineFactory.createEngineDescription(
				DumpOutputAE.class, typeSystem);

		// SimplePipeline.runPipeline(reader, sectionAnnotator, dumpOutput);
		JCasIterable casIter = new JCasIterable(reader, sectionAnnotator, dumpOutput);
		JCasIterator casIt = casIter.iterator();

		assertTrue(casIt.hasNext());
		JCas jCas = casIt.next();

		// DEBUG: TestCDASegmentAnnotator.printSegments(jCas);

		// iterate through segments
		Collection<Segment> segments = JCasUtil.select(jCas, Segment.class);
		assertEquals("No. of segments are provided by: ctakes-regression-test/testdata/input/plaintext/doc2_07543210_sample_current.txt",
				6, segments.size());

		Iterator<Segment> segIt = segments.iterator();

		Segment segment = segIt.next();
		assertNotNull("Segment (0) should not be null", segment);
		assertEquals("2.16.840.1.113883.10.20.22.1.1", segment.getId());
		assertEquals(92, segment.getBegin());
		assertEquals(159, segment.getEnd());
		assertEquals("Header", segment.getPreferredText());

		segment = segIt.next();
		assertNotNull("Segment (1) should not be null", segment);
		assertEquals("1.3.6.1.4.1.19376.1.5.3.1.1.13.2.1", segment.getId());
		assertEquals(176, segment.getBegin());
		assertEquals(1612, segment.getEnd());
		assertEquals("CHIEF COMPLAINT", segment.getPreferredText());

		segment = segIt.next();
		assertNotNull("Segment (2) should not be null", segment);
		assertEquals("2.16.840.1.113883.10.20.22.2.20", segment.getId());
		assertEquals(1634, segment.getBegin());
		assertEquals(1696, segment.getEnd());
		assertEquals("HISTORY OF PAST ILLNESS", segment.getPreferredText());

		segment = segIt.next();
		assertNotNull("Segment (3) should not be null", segment);
		assertEquals("2.16.840.1.113883.10.20.22.2.2.1", segment.getId());
		assertEquals(1711, segment.getBegin());
		assertEquals(2271, segment.getEnd());
		assertEquals("History of immunizations", segment.getPreferredText());

		segment = segIt.next();
		assertNotNull("Segment (4) should not be null", segment);
		assertEquals("2.16.840.1.113883.10.20.22.2.1.1", segment.getId());
		assertEquals(2307, segment.getBegin());
		assertEquals(3506, segment.getEnd());
		assertEquals("HISTORY OF MEDICATION USE", segment.getPreferredText());

		segment = segIt.next();
		assertNotNull("Segment (5) should not be null", segment);
		assertEquals("2.16.840.1.113883.10.20.22.2.15", segment.getId());
		assertEquals(3522, segment.getBegin());
		assertEquals(5608, segment.getEnd());
		assertEquals("Family History", segment.getPreferredText());

		assertFalse("No other jCas should be found", casIt.hasNext());
	}

	public static class DumpOutputAE extends JCasAnnotator_ImplBase {
		public void process(JCas jCas) throws AnalysisEngineProcessException {
			TestCDASegmentAnnotator.printSegments(jCas);
		}
	}
}
