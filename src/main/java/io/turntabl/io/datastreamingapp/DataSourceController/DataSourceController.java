package io.turntabl.io.datastreamingapp.DataSourceController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.turntabl.io.datastreamingapp.DataSource.DataSourceService;
import io.turntabl.io.datastreamingapp.ReTweetTO;
import io.turntabl.io.datastreamingapp.TweeTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api
@RestController
public class DataSourceController {

    @ApiOperation("Get All Profolic Tweeters")
    @CrossOrigin(origins = "*")
    @GetMapping("/v1/api/profolic_tweeters")
    public List<TweeTO> getAllProfolicTweeters() {
        DataSourceService dataSourceService = new DataSourceService();
        return dataSourceService.getAllProfolicTweeters();
    }

    @ApiOperation("Get All Profolic ReTweeters")
    @CrossOrigin(origins = "*")
    @GetMapping("/v1/api/profolic_retweeters")
    public List<ReTweetTO> getAllProfolicReTweeters() {
        DataSourceService dataSourceService = new DataSourceService();
        return dataSourceService.getAllProfolicReTweeters();
    }
}
