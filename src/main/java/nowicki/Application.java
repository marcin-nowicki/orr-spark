package nowicki;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class Application {

    public static void main(String[] args) {

        // Initialize configuration.
        final SparkConf sparkConf = new SparkConf();
        final ApplicationProperties properties = new Configuration().getApplicationProperties();
        sparkConf.setMaster("local")
                .setAppName("wordCount")
                .set("spark.executor.instances", properties.getSparkExecutorInstances())
                .set("spark.executor.cores", properties.getSparkExecutorCores());
        ;

        // Init spark context.
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Let the show begin!
        final JavaPairRDD<String, Integer> wordCount = sc.textFile(properties.getInputFilePath())
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        // Sort results and provide top x:
        final Map<String, Integer> wordCountMap = wordCount.collectAsMap()
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(properties.getResultLimit())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        //Close context.
        sc.close();

        // Print result.
        log.info("Words with the highest occurrence, in descending order:");
        wordCountMap.forEach((word, count) -> log.info("word: \"" + word + "\", counts: " + count));

    }

}