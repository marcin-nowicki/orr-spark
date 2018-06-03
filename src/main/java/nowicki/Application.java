package nowicki;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class Application {

    public static void main(String[] args) {
        //Initialize default params. TODO: add Apache Common CLI params.
        final String inputFilePath = "src/main/resources/lorem-ipsum.txt";
        final Integer resultSize = 10;

        // Init config.
        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local")
                .setAppName("wordCount");

        // Init spark context.
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Let the show begin!
        final JavaPairRDD<String, Integer> wordCount = sc.textFile(inputFilePath)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        // Sort results and provide top x:
        final Map<String, Integer> wordCountMap = wordCount.collectAsMap()
                .entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(resultSize)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        //Close context.
        sc.close();

        // Print result.
        System.out.println("Most common words, in descending order:");
        wordCountMap.forEach((word, count) -> System.out.println("word: \"" + word + "\", counts: " + count));

    }

}
