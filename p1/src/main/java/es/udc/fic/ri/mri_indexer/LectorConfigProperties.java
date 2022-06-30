package es.udc.fic.ri.mri_indexer;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * 
 * @author a.vaquero
 * 
 * La clase LectorConfigProperties se ha reutilizado exactamente igual al año pasado para poder leer
 * el archivo config.properties
 *
 */
public class LectorConfigProperties {
	
    private static final String CONFIG_PROPERTIES_FILE= "config.properties";
    private static Map<String, String> parametros;

    private LectorConfigProperties() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static synchronized Map<String, String> getParameters() {

        if (parametros == null) {
            Class<LectorConfigProperties> configurationParametersManagerClass = LectorConfigProperties.class;
            ClassLoader classLoader = configurationParametersManagerClass.getClassLoader();
            InputStream inputStream = classLoader.getResourceAsStream(CONFIG_PROPERTIES_FILE);
            
            Properties properties = new Properties();
            try {
                properties.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            parametros = (Map<String, String>) new HashMap(properties);

        }
        return parametros;

    }

    public static String getParametro(String parametro) {

        return getParameters().get(parametro);

    }

}