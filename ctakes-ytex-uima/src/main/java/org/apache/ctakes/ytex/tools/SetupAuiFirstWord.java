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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.apache.ctakes.core.nlp.tokenizer.Token;
import org.apache.ctakes.core.nlp.tokenizer.TokenizerPTB;
import org.apache.ctakes.ytex.kernel.KernelContextHolder;
import org.apache.ctakes.ytex.umls.dao.UMLSDao;
import org.apache.ctakes.ytex.umls.model.UmlsAuiFirstWord;

import gov.nih.nlm.nls.lvg.Api.LvgCmdApi;

/**
 * setup umls_aui_fword table
 * 
 * @author vijay
 * 
 */
public class SetupAuiFirstWord {

	private static final Log log = LogFactory.getLog(SetupAuiFirstWord.class);

	private static final String LVG_PROPERTIES_PATH = "org/apache/ctakes/lvg/data/config/lvg.properties";
	private static final String LVG_ANNOTATOR_PATH = "ctakes-lvg/desc/analysis_engine/LvgAnnotator.xml";
	private static final String LVG_ANNOTATOR_PATH2 = "../ctakes-lvg/desc/analysis_engine/LvgAnnotator.xml";

	public static final String EXCLUSION_SET = "ExclusionSet";

	// private static final Pattern nonWord = Pattern.compile("\\W");
	private TokenizerPTB tokenizer = new TokenizerPTB();
	private Set<String> exclusionSet = new HashSet<>();
	private LvgCmdApi lvgCmd;

	/**
	 * Initialize tokenizer using the hyphen map from
	 * "tokenizer/hyphenated.txt". Use freqCutoff of 0. If this is changed in
	 * the TokenizerAnnotator.xml uima config, then the tokenization here will
	 * not match the tokenization done during document processing.
	 * <p/>
	 * Initialize exclusionSet from LvgAnnotator.xml. The exclusion set should
	 * be case insensitive, but it isn't that way in the LvgAnnotator so we
	 * retain the same functionality.
	 * <p/>
	 * Initialize LVG. copied from
	 * edu.mayo.bmi.uima.lvg.resource.LvgCmdApiResourceImpl.
	 * 
	 * @throws Exception
	 */
	public SetupAuiFirstWord() throws Exception {
		initExclusionSet();
		initLvg();
	}

	/**
	 * To be used only from UTest.
	 *
	 * @return exclusionSet
	 */
	final Collection<String> getExclusionList() {
		return exclusionSet;
	}

	/**
	 * To be used only from UTest.
	 *
	 * @return LvgCmdApi
	 */
	final LvgCmdApi getLvgCmd() {
		return lvgCmd;
	}

	/**
	 * Initialize LVG
	 *
	 * // TODO: There is no need for uri, f, and folder arithmetic. Consider refactor.
	 */
	private void initLvg() {
		// See http://lexsrv2.nlm.nih.gov/SPECIALIST/Projects/lvg/2008/docs/userDoc/index.html
		// See http://lexsrv3.nlm.nih.gov/SPECIALIST/Projects/lvg/2008/docs/designDoc/UDF/flow/index.html
		// Lower-case the terms and then uninflect
		//      f = using flow components (in this order)
		//      l = lower case
		//      b = uninflect a term
		URL uri = this.getClass().getClassLoader().getResource(LVG_PROPERTIES_PATH);
		log.info(String.format("loading lvg.properties from: %s", uri.getPath()));

		File f = new File(uri.getPath());
		String configDir = f.getParentFile().getAbsolutePath();

		String lvgDir = configDir.substring(0, configDir.length() - "data/config".length());
		log.info(String.format("Using lvgDir %s", lvgDir));
		System.setProperty("user.dir", lvgDir);
		log.info(String.format("Loading lvgCmd from ", f.getAbsolutePath()));
		lvgCmd = new LvgCmdApi("-f:l:b", f.getAbsolutePath());
	}


	/**
	 * Helper function to retrieve LvgAnnotator:InputStream
	 *
	 */
	private final InputStream getLvgAnnotatorInputStream() {
		InputStream is = SetupAuiFirstWord.class.getResourceAsStream(LVG_ANNOTATOR_PATH);
		if (is == null) {
			log.warn(String.format("classpath:%s not available, attempting to load from file system", LVG_ANNOTATOR_PATH));
			File f = new File(LVG_ANNOTATOR_PATH2);
			try {
				is = new BufferedInputStream(new FileInputStream(f));
			} catch (FileNotFoundException e) {
				log.warn(String.format("classpath:%s not available, using empty exclusion set", LVG_ANNOTATOR_PATH2));
			}
		}
		return is;
	}
	/**
	 * initialize lvg exclusion set
	 * 
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	private void initExclusionSet() throws ParserConfigurationException, SAXException, IOException {
		InputStream isLvgAnno = null;
		isLvgAnno = getLvgAnnotatorInputStream();
		if (isLvgAnno != null) {
			DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			try {
				Document doc = dBuilder.parse(isLvgAnno);
				NodeList nList = doc.getElementsByTagName("nameValuePair");
				for (int i = 0; i < nList.getLength(); i++) {
					Element e = (Element) nList.item(i);
					// TODO: possible NullPointerException when no Elements (index: 0) - happy path
					String name = e.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue();
					if (EXCLUSION_SET.equals(name)) {
						NodeList nListEx = e.getElementsByTagName("string");
						for (int j = 0; j < nListEx.getLength(); j++) {
							String nodeValue = nListEx.item(j).getChildNodes().item(0).getNodeValue();
							log.debug(String.format("Adding to %s:", EXCLUSION_SET, nodeValue));
							exclusionSet.add(nodeValue);
						}
					}
				}
			} finally {
				isLvgAnno.close();
			}
		}
	}

	/**
	 * initialize the tokenizer. loads the hypenated word list.
	 */
	private void initTokenizer() {
		this.tokenizer = new TokenizerPTB();
	}

	/**
	 *
	 * @param args - not used
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		SetupAuiFirstWord setupFword = new SetupAuiFirstWord();
		setupFword.setupAuiFirstWord();
	}

	private final UMLSDao getUMLSDaoBean() {
		return KernelContextHolder.getApplicationContext().getBean(UMLSDao.class);
	}

	private final PlatformTransactionManager getPTMBean() {
		return KernelContextHolder.getApplicationContext().getBean(PlatformTransactionManager.class);
	}

	private final TransactionTemplate createTransactionTemplate() {
		TransactionTemplate t = new TransactionTemplate( getPTMBean() );
		t.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
		return t;
	}

	/**
	 * Main method
	 */
	public void setupAuiFirstWord() {
		UMLSDao umlsDao = getUMLSDaoBean();
		TransactionTemplate t = createTransactionTemplate();

		// TODO: investigate if needed to delete all records
		// delete all records
		// umlsDao.deleteAuiFirstWord();

		// 1. get all AUIs and their strings
		// 2. restart processing after the last AUI we processed.
		// if lastAUI is null, then just process everything
		String lastAui = umlsDao.getLastAui();

		List<Object[]> listAuiStr = null;
		do {
			// get the next 10k auis
			listAuiStr = umlsDao.getAllAuiStr(lastAui);
			// put the aui - fword pairs in a list
			List<UmlsAuiFirstWord> listFword = new ArrayList<UmlsAuiFirstWord>(1000);
			for (Object[] auiStr : listAuiStr) {
				String aui = (String) auiStr[0];
				String str = (String) auiStr[1];
				// save lastAUI processed
				lastAui = aui;

				if (str.length() < 200) {
					try {
						UmlsAuiFirstWord fw = this.tokenizeStr(aui, str);
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
				umlsDao.insertAuiFirstWord(listFword);
				log.info("inserted " + listFword.size() + " rows");
			}
		} while (listAuiStr.size() > 0);
	}

	/**
	 * tokenize the umls concept. copied from
	 * edu\mayo\bmi\dictionarytools\CreateLuceneIndexFromDelimitedFile.java.
	 * 
	 * Stem the concept. Stemming performed analogous to LvgAnnotator.
	 * 
	 * @param aui
	 * @param str
	 * @return
	 * @throws Exception
	 */
	public UmlsAuiFirstWord tokenizeStr(String aui, String str) throws Exception {
		List<?> list = tokenizer.tokenize(str);
		Iterator<?> tokenItr = list.iterator();
		int tCount = 0;
		String firstTokenText = "";
		StringBuilder tokenizedDesc = new StringBuilder();
		String firstTokenStem = "";
		StringBuilder stemmedDesc = new StringBuilder();

		// get first word token and
		while (tokenItr.hasNext()) {
			tCount++;
			Token t = (Token) tokenItr.next();
			if (tCount == 1) {
				firstTokenText = t.getText(); // first token (aka "first word")
				tokenizedDesc.append(firstTokenText);
				if (this.lvgCmd != null) {
					firstTokenStem = stemToken(t);
					stemmedDesc.append(firstTokenStem);
				}
			} else { // use blank to separate tokens
				tokenizedDesc.append(" ").append(t.getText());
				// stem the next token, add it to the stemmed desc only if there
				// is a valid first word
				if (this.lvgCmd != null && firstTokenStem != null) {
					String stemmedWord = stemToken(t);
					stemmedDesc.append(" ").append(stemmedWord);
				}
			}
		}
		UmlsAuiFirstWord fw = new UmlsAuiFirstWord();
		fw.setAui(aui);
		fw.setFword(firstTokenText.toLowerCase(Locale.ENGLISH));
		fw.setTokenizedStr(tokenizedDesc.toString());
		if (this.lvgCmd != null) {
			fw.setFstem(firstTokenStem.toLowerCase(Locale.ENGLISH));
			fw.setStemmedStr(stemmedDesc.toString());
		}
		return fw;
	}

	/**
	 * 
	 * @param t
	 *            token
	 * @return stemmed text if token is a word and stemmed text is non-empty.
	 *         else raw token text.
	 * @throws Exception
	 */
	private String stemToken(Token t) throws Exception {
		String stemmedWord = t.getText();
		if (Token.TYPE_WORD == t.getType() || Token.TYPE_UNKNOWN == t.getType()) {
			stemmedWord = this.getCanonicalForm(t.getText());
			if (stemmedWord == null || stemmedWord.length() == 0) {
				stemmedWord = t.getText();
			}
		}
		return stemmedWord;
	}

	/**
	 * copied from edu.mayo.bmi.uima.lvg.ae.LvgAnnotator
	 * 
	 * @param word
	 * @return
	 * @throws Exception
	 */
	private String getCanonicalForm(String word) throws Exception {
		if (lvgCmd == null || this.exclusionSet.contains(word))
			return null;
		String canonicalForm = null;
		String out = lvgCmd.MutateToString(word);
		// vng null check
		String[] output = null;
		if (out != null)
			output = out.split("\\|");
		else {
			log.warn("mutateToString returned null for: " + word);
		}

		if ((output != null) && (output.length >= 2)
				&& (!output[1].matches("No Output"))) {
			canonicalForm = output[1];
		}
		return canonicalForm;
	}
}
