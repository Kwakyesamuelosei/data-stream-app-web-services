package io.turntabl.io.datastreamingapp.DataSource;

import io.turntabl.io.datastreamingapp.ReTweetTO;
import io.turntabl.io.datastreamingapp.TweeTO;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json4s.DefaultFormats;
import org.json4s.jackson.Serialization;
import org.springframework.context.annotation.Bean;
import scala.util.control.Exception;
import scala.util.parsing.json.JSONObject$;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;


public class DataSourceService {

    public List<TweeTO> getAllProfolicTweeters()  {
        List<TweeTO> results = new ArrayList<>();
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", "hdfs://ec2-18-217-53-111.us-east-2.compute.amazonaws.com:8181");
            FileSystem fileSystem = FileSystem.get(configuration);
//            SparkContext sparkContext = new SparkContext(new SparkConf().setAppName("DataSource").set("spark.driver.allowMultipleContexts","true").setMaster("local"));
            spark.conf().set("spark.driver.memory", 571859200);
            SparkSession spark = SparkSession.builder().master("local").appName("demo").getOrCreate();
            JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

            FileStatus[] status = fileSystem.listStatus(new Path("/mydata/"));
            List<String> jsonList = new ArrayList<>();
            for (int i=0;i<status.length;i++){
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                    line=br.readLine();
                    jsonList.add(line);
                }
            }
            JavaRDD<String> javaRdd = javaSparkContext.parallelize(jsonList);
            Dataset<Row> data = spark.read().option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(javaRdd);
//            data.registerTempTable("tweets");
            data.cache();
            data.registerTempTable("tweets");

            List<TweeTO> resultSet = spark.sql("select name, count(id) from tweets group by name").collectAsList().stream()
                    .map(row -> tweetMapper(row))
                    .collect(Collectors.toList());
            for (TweeTO row: resultSet){
                results.add(row);
            }

        }catch (IOException e){
            e.printStackTrace();
        }
      return results;
    }
    public List<ReTweetTO> getAllProfolicReTweeters()  {
        List<ReTweetTO> results = new ArrayList<>();
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", "hdfs://ec2-18-217-53-111.us-east-2.compute.amazonaws.com:8181");
            FileSystem fileSystem = FileSystem.get(configuration);
//            SparkContext sparkContext = new SparkContext(new SparkConf().setAppName("DataSource").set("spark.driver.allowMultipleContexts","true").setMaster("local"));
            spark.conf().set("spark.driver.memory", 571859200);
            SparkSession spark = SparkSession.builder().master("local").appName("demo").getOrCreate();
            JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

            FileStatus[] status = fileSystem.listStatus(new Path("/mydata/"));
            List<String> jsonList = new ArrayList<>();
            for (int i=0;i<status.length;i++){
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                    line=br.readLine();
                    jsonList.add(line);
                }
            }
            JavaRDD<String> javaRdd = javaSparkContext.parallelize(jsonList);
            Dataset<Row> data = spark.read().option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(javaRdd);
//            data.registerTempTable("tweets");
            data.cache();
            data.registerTempTable("tweets");

            List<ReTweetTO> resultSet = spark.sql("select name,reTweetCount from tweets where reTweetCount > 5  group by name").collectAsList().stream()
                    .map(row -> retweetMapper(row))
                    .collect(Collectors.toList());
            for (ReTweetTO row: resultSet){
                results.add(row);
            }

        }catch (IOException e){
            e.printStackTrace();
        }
        return results;
    }

    public TweeTO tweetMapper(Row row){
        TweeTO tweeTO = new TweeTO();

        try{

            String screenName;
            Long tweetCount;
            if(row.getString(0) == null){
                screenName = "";
            }else {
                screenName = row.getString(0);
            }
            if(row.get(1) == null){
                tweetCount = 0L;
            }else {
                tweetCount = row.getLong(1);
            }
            tweeTO.setScreenName(screenName);
            tweeTO.setTweetCount(tweetCount);

        }catch (java.lang.Exception e){
            e.printStackTrace();
        }
        return tweeTO;
    }

    public ReTweetTO retweetMapper(Row row){
        ReTweetTO reTweetTO = new ReTweetTO();

        try{

            String screenName;
            Long retweetCount;
            if(row.getString(0) == null){
                screenName = "";
            }else {
                screenName = row.getString(0);
            }
            if(row.get(1) == null){
                retweetCount = 0L;
            }else {
                retweetCount = row.getLong(1);
            }
            reTweetTO.setScreenName(screenName);
            reTweetTO.setRetweetCount(retweetCount);

        }catch (java.lang.Exception e){
            e.printStackTrace();
        }
        return reTweetTO;
    }
}


