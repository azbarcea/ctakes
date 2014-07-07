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
package org.apache.ctakes.temporal.ae;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.ctakes.relationextractor.ae.RelationExtractorAnnotator;
import org.apache.ctakes.relationextractor.ae.features.PartOfSpeechFeaturesExtractor;
import org.apache.ctakes.relationextractor.ae.features.RelationFeaturesExtractor;
import org.apache.ctakes.relationextractor.ae.features.TokenFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.CheckSpecialWordRelationExtractor;
import org.apache.ctakes.temporal.ae.feature.DependencyPathFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.EventArgumentPropertyExtractor;
import org.apache.ctakes.temporal.ae.feature.NearbyVerbTenseRelationExtractor;
import org.apache.ctakes.temporal.ae.feature.SectionHeaderRelationExtractor;
import org.apache.ctakes.temporal.ae.feature.UmlsFeatureExtractor;
import org.apache.ctakes.typesystem.type.relation.BinaryTextRelation;
import org.apache.ctakes.typesystem.type.relation.RelationArgument;
import org.apache.ctakes.typesystem.type.relation.TemporalTextRelation;
import org.apache.ctakes.typesystem.type.textsem.EventMention;
import org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation;
import org.apache.ctakes.typesystem.type.textspan.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.cleartk.classifier.CleartkAnnotator;
import org.cleartk.classifier.DataWriter;
import org.cleartk.classifier.jar.DefaultDataWriterFactory;
import org.cleartk.classifier.jar.DirectoryDataWriterFactory;
import org.cleartk.classifier.jar.GenericJarClassifierFactory;
import org.uimafit.factory.AnalysisEngineFactory;
import org.uimafit.util.JCasUtil;

import com.google.common.collect.Lists;

public class EventEventRelationAnnotator extends RelationExtractorAnnotator {

	  public static AnalysisEngineDescription createDataWriterDescription(
		      Class<? extends DataWriter<String>> dataWriterClass,
		      File outputDirectory,
		      double probabilityOfKeepingANegativeExample) throws ResourceInitializationException {
		    return AnalysisEngineFactory.createPrimitiveDescription(
		            EventEventRelationAnnotator.class,
		            CleartkAnnotator.PARAM_IS_TRAINING,
		            true,
		            DefaultDataWriterFactory.PARAM_DATA_WRITER_CLASS_NAME,
		            dataWriterClass,
		            DirectoryDataWriterFactory.PARAM_OUTPUT_DIRECTORY,
		            outputDirectory,
		            RelationExtractorAnnotator.PARAM_PROBABILITY_OF_KEEPING_A_NEGATIVE_EXAMPLE,
		            // not sure why this has to be cast; something funny going on in uimaFIT maybe?
		            (float) probabilityOfKeepingANegativeExample);
		      }
	  
	  public static AnalysisEngineDescription createAnnotatorDescription(String modelPath)
		      throws ResourceInitializationException {
		    return AnalysisEngineFactory.createPrimitiveDescription(
		        EventEventRelationAnnotator.class,
		        CleartkAnnotator.PARAM_IS_TRAINING,
		        false,
		        GenericJarClassifierFactory.PARAM_CLASSIFIER_JAR_PATH,
		        modelPath);
		  }
	  
	  /**
	   * @deprecated use String path instead of File.
	   * ClearTK will automatically Resolve the String to an InputStream.
	   * This will allow resources to be read within from a jar as well as File.  
	   */	  
	  public static AnalysisEngineDescription createAnnotatorDescription(File modelDirectory)
		      throws ResourceInitializationException {
		    return AnalysisEngineFactory.createPrimitiveDescription(
		        EventEventRelationAnnotator.class,
		        CleartkAnnotator.PARAM_IS_TRAINING,
		        false,
		        GenericJarClassifierFactory.PARAM_CLASSIFIER_JAR_PATH,
		        new File(modelDirectory, "model.jar"));
		  }

	  @Override
	  protected List<RelationFeaturesExtractor> getFeatureExtractors() {
	    return Lists.newArrayList(
	    						  new TokenFeaturesExtractor()
	    						, new PartOfSpeechFeaturesExtractor()
//	    						, new EventArgumentPropertyExtractor()
	    						, new SectionHeaderRelationExtractor()
	    						, new NearbyVerbTenseRelationExtractor()
	    						, new CheckSpecialWordRelationExtractor()
	    						, new UmlsFeatureExtractor()
	    						, new DependencyPathFeaturesExtractor()
	    						);
	  }

	@Override
	protected Class<? extends Annotation> getCoveringClass() {
		return Sentence.class;
	}

	@Override
	protected List<IdentifiedAnnotationPair> getCandidateRelationArgumentPairs(
			JCas jCas, Annotation sentence) {
	    List<IdentifiedAnnotationPair> pairs = Lists.newArrayList();
	    List<EventMention> events = new ArrayList<EventMention>(JCasUtil.selectCovered(jCas, EventMention.class, sentence));
	    for (int i = 0; i < events.size(); i++){
	      if(!events.get(i).getClass().equals(EventMention.class)) continue;
	    	for(int j = i+1; j < events.size(); j++){
	    	  if(!events.get(j).getClass().equals(EventMention.class)) continue;
	        pairs.add(new IdentifiedAnnotationPair(events.get(i), events.get(j)));
	    	}
	    }
	    return pairs;
	}
	
	@Override
	protected void createRelation(JCas jCas, IdentifiedAnnotation arg1,
			IdentifiedAnnotation arg2, String predictedCategory) {
		RelationArgument relArg1 = new RelationArgument(jCas);
		relArg1.setArgument(arg1);
		relArg1.setRole("Arg1");
		relArg1.addToIndexes();
		RelationArgument relArg2 = new RelationArgument(jCas);
		relArg2.setArgument(arg2);
		relArg2.setRole("Arg2");
		relArg2.addToIndexes();
		TemporalTextRelation relation = new TemporalTextRelation(jCas);
		relation.setArg1(relArg1);
		relation.setArg2(relArg2);
		relation.setCategory(predictedCategory);
		relation.addToIndexes();
	}
	
	@Override
	protected String getRelationCategory(
			Map<List<Annotation>, BinaryTextRelation> relationLookup,
			IdentifiedAnnotation arg1,
			IdentifiedAnnotation arg2) {
		BinaryTextRelation relation = relationLookup.get(Arrays.asList(arg1, arg2));
		String category = null;
		if (relation != null) {
			category = relation.getCategory();
		} else {
			relation = relationLookup.get(Arrays.asList(arg2, arg1));
			if (relation != null) {
				if(relation.getCategory().equals("OVERLAP")){
					category = relation.getCategory();
				}else{
					category = relation.getCategory() + "-1";
				}
			}
		}
		if (category == null && coin.nextDouble() <= this.probabilityOfKeepingANegativeExample) {
			category = NO_RELATION_CATEGORY;
		}
		return category;
	}
}