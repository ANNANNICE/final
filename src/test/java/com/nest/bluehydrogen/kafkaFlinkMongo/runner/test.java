package com.nest.bluehydrogen.kafkaFlinkMongo.runner;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features ="src/test/resources/features/flinkMongo.feature",glue="com.nest.bluehydrogen.kafkaFlinkMongo.stepdef")
public class test {
}
