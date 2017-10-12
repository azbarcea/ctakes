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
package org.apache.ctakes.ytex.tools;

import org.apache.ctakes.ytex.kernel.KernelContextHolder;
import org.apache.ctakes.ytex.umls.dao.UMLSDao;
import org.apache.ctakes.ytex.umls.model.UmlsAuiFirstWord;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SetupAuiFirstWordTest {

	private static final Logger log = Logger.getLogger(SetupAuiFirstWord.class);

	private static UMLSDao beanUmlsDao;
	private static PlatformTransactionManager beanPTM;

	private SetupAuiFirstWord helperSetupFirstWord;
	private TransactionTemplate transactionTemplate;

	@BeforeClass
	public static void setUp() {
		// umlsDao is used in SetupAuiFirstWord.setupAuiFirstWord();
		beanUmlsDao = KernelContextHolder.getApplicationContext().getBean(UMLSDao.class);
		beanPTM = KernelContextHolder.getApplicationContext().getBean(PlatformTransactionManager.class);
	}

	@Before
	public void initializeSetupAuiFirstWord() throws Exception {
		helperSetupFirstWord = new SetupAuiFirstWord();
		transactionTemplate = new TransactionTemplate(beanPTM);
		transactionTemplate.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
	}

	@After
	public  void cleanSetupAuiFirstWord() {
		this.helperSetupFirstWord = null;
	}

	@Test
	public void testBeansInitializaitons() {
		assertNotNull(beanUmlsDao);
		assertNotNull(beanPTM);
	}

	/**
	 * Helper function to assert both Word lowercase Word.
	 *
	 * @param list
	 * @param word
	 */
	private final void assertCaseInsensitive(final Collection list, final String word) {
		assertTrue(list.contains(word));
		assertTrue(list.contains(word.toLowerCase()));
	}

	@Test
	public void testInitialization() {
		assertNotNull("Could not initialize SetupAuiFirstWord", helperSetupFirstWord);

		log.info(String.format("Testing %s initialization", SetupAuiFirstWord.EXCLUSION_SET));
		Collection<String> exclusionList = helperSetupFirstWord.getExclusionList();
		assertCaseInsensitive(exclusionList, "And");
		assertCaseInsensitive(exclusionList, "By");
		assertCaseInsensitive(exclusionList, "For");
		assertCaseInsensitive(exclusionList, "In");
		assertCaseInsensitive(exclusionList, "Of");
		assertCaseInsensitive(exclusionList, "On");
		assertCaseInsensitive(exclusionList, "The");
		assertCaseInsensitive(exclusionList, "To");
		assertCaseInsensitive(exclusionList, "With");
		// no other elements should be excluded
		assertEquals(String.format("Expecting %d exclusing elements", 18), 18, exclusionList.size());

		log.info("Testing LVG initialization");
		assertNotNull("Expecting a valid LVG Command", helperSetupFirstWord.getLvgCmd());

		log.info("Testing TransactionTemplate");
		assertNotNull(transactionTemplate);
	}

	@Test
	public void testTokenizer() throws Exception {
		UmlsAuiFirstWord auiFirstWord = helperSetupFirstWord.tokenizeStr("2tok", "heart attack");
		assertNotNull(auiFirstWord);
		assertTrue(auiFirstWord.getFword().equals("heart"));
	}

	@Test
	public void testTransactionTemplateInitialization() {
		TransactionTemplate t = new TransactionTemplate(beanPTM);
		t.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
		assertNotNull(t);
	}

	@Test
	public void testSetupAuiFirstWord() throws Exception {
		// TODO: investigate if needed to delete all records
		// delete all records
		// umlsDao.deleteAuiFirstWord();

		// 1. get all AUIs and their strings
		// 2. restart processing after the last AUI we processed.
		// if lastAUI is null, then just process everything
		String lastAui = beanUmlsDao.getLastAui();

		List<Object[]> listAuiStr = null;
		do {
			// get the next 10k auis
			listAuiStr = beanUmlsDao.getAllAuiStr(lastAui);
			// put the aui - fword pairs in a list
			List<UmlsAuiFirstWord> listFword = new ArrayList<UmlsAuiFirstWord>(1000);
			for (Object[] auiStr : listAuiStr) {
				String aui = (String) auiStr[0];
				String str = (String) auiStr[1];
				// save lastAUI processed
				lastAui = aui;

				if (str.length() < 200) {
					try {
						UmlsAuiFirstWord fw = helperSetupFirstWord.tokenizeStr(aui, str);
						// TODO: Too many if/else. Consider refactor.
						if (fw == null) {
							log.error(String.format("Error tokenizing aui=%s, str=%s", aui, str));
						} else if (fw.getFword().length() > 70) {
							log.debug(String.format("fword too long: aui=%s, str=%s", aui, fw.getFword()));
						} else if (fw.getTokenizedStr().length() > 250) {
							log.debug(String.format("String too long: aui=%s, str=%s",  aui, str));
						} else {
							log.debug(String.format("aui=%s, fw=%s", aui, fw));
							listFword.add(fw);
						}
					} catch (Exception e) {
						log.error(String.format("Error tokenizing aui=%s, str=%s", aui, str), e);
					}
				} else {
					log.debug(String.format("Skipping aui because str to long: aui=%s, str=%s",  aui, str));
				}
			}
			// batch insert
			if (listFword.size() > 0) {
				// HACK: Shouldn't update umlsDao internal state and not reuse it later. possible BUG
				//  inserted at the end of the while loop, retrieved at the beginning fo the while loop
				beanUmlsDao.insertAuiFirstWord(listFword);
				log.info("inserted " + listFword.size() + " rows");
			}
		} while (listAuiStr.size() > 0);

		log.info("SetupAuiFirstWord completed processing without error");
	}



}
