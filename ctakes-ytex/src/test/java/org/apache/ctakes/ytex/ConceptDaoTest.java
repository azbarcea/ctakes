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
package org.apache.ctakes.ytex;

import org.apache.ctakes.ytex.kernel.SimSvcContextHolder;
import org.apache.ctakes.ytex.kernel.dao.ConceptDao;
import org.apache.ctakes.ytex.kernel.metric.ConceptPairSimilarity;
import org.apache.ctakes.ytex.kernel.metric.ConceptSimilarityService;
import org.apache.ctakes.ytex.kernel.metric.ConceptSimilarityService.SimilarityMetricEnum;
import org.apache.ctakes.ytex.kernel.model.ConceptGraph;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.access.ContextSingletonBeanFactoryLocator;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConceptDaoTest {

	static private final Logger LOGGER = Logger.getLogger(ConceptDaoTest.class);

	private ConceptDao conceptDao = null;

	private BeanFactory appCtx = null;

	@Before
	public void setUp() throws Exception {
		appCtx = ContextSingletonBeanFactoryLocator
				.getInstance("classpath*:/org/apache/ctakes/ytex/kernelBeanRefContext.xml")
				.useBeanFactory("kernelApplicationContext").getFactory();

		conceptDao = appCtx.getBean(ConceptDao.class);

		JdbcTemplate jdbcTemplate = new JdbcTemplate();
		jdbcTemplate.setDataSource(appCtx.getBean(DataSource.class));
		Properties ytexProperties = (Properties)appCtx.getBean("ytexProperties");
		String dbtype = ytexProperties.getProperty("db.type");
		if("hsql".equals(dbtype) || "mysql".equals(dbtype))
			jdbcTemplate.execute("drop table if exists test_concepts");
		if("mssql".equals(dbtype))
			jdbcTemplate.execute("if exists(select * from sys.objects where object_id = object_id('test_concepts')) drop table test_concepts");
		if ("orcl".equals(dbtype)) {
			// just try dropping the table, catch exception and hope all is well
			try {
				jdbcTemplate.execute("drop table test_concepts");
			} catch (Exception ignore) {
				LOGGER.warn("couldn't drop table test_concepts", ignore);
			}
		}
		jdbcTemplate.execute("create table test_concepts(parent varchar(20), child varchar(20))");
		jdbcTemplate.execute("insert into test_concepts values ('root', 'animal')");
		jdbcTemplate.execute("insert into test_concepts values ('animal', 'vertebrate')");
		jdbcTemplate.execute("insert into test_concepts values ('vertebrate', 'cat')");
		jdbcTemplate.execute("insert into test_concepts values ('vertebrate', 'dog')");
		jdbcTemplate.execute("insert into test_concepts values ('root', 'bacteria')");
		jdbcTemplate.execute("insert into test_concepts values ('bacteria', 'e coli')");
		LOGGER.info("Create concept graph");
		conceptDao.createConceptGraph(null, "test",
				"select child, parent from test_concepts", true,
				Collections.EMPTY_SET);
		ConceptGraph cg = conceptDao.getConceptGraph("test");
		assertNotNull(cg);
		((ConfigurableApplicationContext) appCtx).close();
	}

	@Test
	public void testCreateConceptGraph() throws IOException {
		System.setProperty("ytex.conceptGraphName", "test");
		System.setProperty("ytex.conceptPreload", "false");
		System.setProperty("ytex.conceptSetName", "");
		// ApplicationContext appCtxSim = new ClassPathXmlApplicationContext(
		// new String[] { "org/apache/ctakes/ytex/beans-kernel-sim.xml" },
		// appCtx);
		// ConceptSimilarityService simSvc = appCtxSim
		// .getBean(ConceptSimilarityService.class);
		ConceptSimilarityService simSvc = SimSvcContextHolder
				.getApplicationContext()
				.getBean(ConceptSimilarityService.class);
		ConceptPairSimilarity simDogCat = simSvc.similarity(
				Arrays.asList(SimilarityMetricEnum.PATH,
						SimilarityMetricEnum.INTRINSIC_PATH), "dog", "cat",
				null, false);
		ConceptPairSimilarity simDogEColi = simSvc.similarity(
				Arrays.asList(SimilarityMetricEnum.PATH,
						SimilarityMetricEnum.INTRINSIC_PATH), "dog", "e coli",
				null, false);
		assertTrue(simDogCat.getSimilarities().get(0) > simDogEColi.getSimilarities().get(0));
		assertTrue(simDogCat.getSimilarities().get(1) > simDogEColi.getSimilarities().get(1));
	}

}
