package nowicki;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Configuration {
    @Getter
    private ApplicationProperties applicationProperties;

    public Configuration() {
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            final Properties properties = new Properties();
            properties.load(inputStream);

            this.applicationProperties = ApplicationProperties.builder()
                    .inputFilePath(properties.getProperty("input.path"))
                    .resultLimit(Integer.parseInt(properties.getProperty("result.size")))
                    .sparkExecutorCores(properties.getProperty("spark.executor.instances"))
                    .sparkExecutorInstances(properties.getProperty("spark.executor.cores"))
                    .build();

        } catch (IOException e) {
            log.error("Failed to load properties! ", e.getStackTrace());
        }
    }

}

@Getter
@Builder
class ApplicationProperties {
    private String inputFilePath;
    private Integer resultLimit;
    private String sparkExecutorInstances;
    private String sparkExecutorCores;
}
