package io.cozmic.usher;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import io.vertx.core.json.JsonObject;

import java.nio.file.Paths;

/**
 * Provides a "Convention over Configuration" approach to config files for usher.
 *
 * Services built with usher can put an application.conf file in the classpath and call it good.
 * However, they can also use the USHER_ENV environment variable to specify a runtime environment
 * and load/merge values from other config files.
 * @return
 */
public class ConfigLoader {

    private final JsonObject runtimeOverrideConfig;
    private String configFile;
    private String envOverrideKey;
    private String basePackage;

    public static ConfigLoader usherDefault(JsonObject runtimeOverrideConfig) {
        // Allow overriding of application.conf as the main usher file.
        final Object overrideUsherBasePackage = runtimeOverrideConfig.remove("usherBasePackage");
        final String usherBasePackage = overrideUsherBasePackage != null ? (String)overrideUsherBasePackage : "";
        final Object overrideUsherConfigFile = runtimeOverrideConfig.remove("usherConfigFile");
        final String usherConfigFile = overrideUsherConfigFile != null ? (String)overrideUsherConfigFile : "application";

        return new ConfigLoader(runtimeOverrideConfig, usherConfigFile, usherBasePackage);
    }


    public ConfigLoader(JsonObject runtimeOverrideConfig, String configFile, String basePackage) {
        this(runtimeOverrideConfig, configFile, basePackage, "USHER_ENV");
    }

    public ConfigLoader(JsonObject runtimeOverrideConfig, String configFile, String basePackage, String envOverrideKey) {
        this.runtimeOverrideConfig = runtimeOverrideConfig;
        this.configFile = configFile;
        this.envOverrideKey = envOverrideKey;
        this.basePackage = basePackage;
    }

    public JsonObject buildUsherConfig() {
        //https://github.com/typesafehub/config#standard-behavior
        final String referencePath = Paths.get(basePackage, "reference").toString();
        final String configFilePath = Paths.get(basePackage, configFile).toString();

        final Config refConfig = ConfigFactory.parseResourcesAnySyntax(referencePath);
        final Config defaultConfig = ConfigFactory.parseResourcesAnySyntax(configFilePath);

        //load a production.conf if any
        String env = System.getenv(envOverrideKey);
        if (env == null) {
            env = "production";
        }
        final String envConfigFilePath = Paths.get(basePackage, String.format("%s.conf", env)).toString();
        final Config envConfig = ConfigFactory.parseResourcesAnySyntax(envConfigFilePath);

        JsonObject runtimeConfig = runtimeOverrideConfig;
        if (runtimeConfig == null) {
            runtimeConfig = new JsonObject();
        }

        final Config runtimeOverrides = ConfigFactory.parseString(runtimeConfig.toString(), ConfigParseOptions.defaults());
        Config resolvedConfigs;
        resolvedConfigs = runtimeOverrides
                .withFallback(envConfig)
                .withFallback(defaultConfig)
                .withFallback(refConfig)
                .resolve();


        return new JsonObject(resolvedConfigs.root().render(ConfigRenderOptions.concise()));
    }
}
