package io.turntabl.io.datastreamingapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.lang.reflect.Array;

@EnableSwagger2
//@ComponentScan(basePackages = List("io.turntabl.*"))
@SpringBootApplication
public class DataStreamingAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataStreamingAppApplication.class, args);
	}

}
