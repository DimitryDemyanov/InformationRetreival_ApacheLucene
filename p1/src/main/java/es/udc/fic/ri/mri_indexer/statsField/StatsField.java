package es.udc.fic.ri.mri_indexer.statsField;


import java.io.IOException;
import java.nio.file.Paths;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class StatsField {

    public static void main(String[] args) throws IOException {

    	String usage = "StatsField:/n" 
				+ "[-index INDEX_PATH]\n"
				+ "[-field NOMBRE_CAMPO]\n";	    

        String indexPath = null;
        String field = null;        
            
        for (int i = 0;i<args.length;i++) {
        	if ("-index".contentEquals(args[i])) {
        		indexPath = args[i+1];
        		i++;
        	} else if ("-field".contentEquals(args[i])) {
        		field = args[i+1];
        		i++;
        	} else if ((args.length != 1) | ("-help".contentEquals(args[i]))) {
				System.out.println(usage);
				System.exit(1);
			}
        }
        
        //Comprobación de que se pasa "-index" (es obligatorio)
        if (indexPath == null) {
        	System.out.println("ERROR: No se ha especificado el \"index_path\" del directorio a indexar");
        	System.out.println(usage);
			System.exit(1);
        }

      
        // Inicializamos IndexReader y comprobamos que la apertura de la carpeta index sea satisfactoria
        Directory dir = null;
        DirectoryReader indexReader = null;

            
        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
        } catch (CorruptIndexException e1) {
        	System.out.println("Graceful message: exception " + e1);
        	e1.printStackTrace();
        } catch (IOException e1) {
        	System.out.println("Graceful message: exception " + e1);
        	e1.printStackTrace();
        }
        

        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        System.out.printf("%-30s%-20s%-20s%-20s%-20s\n", "FIELD", "DOC_COUNT", "MAX_DOC", "SUM_DOC_FREQ",
                "SUM_TOTAL_TERM_FREQ");

        //Si no le pasan el -field no lee el campo por lo que se queda inicializado en ""
        //Con el bucle for se muestran las estadisticas de cada campo
        if (field == null) {    
            final FieldInfos fieldinfos = FieldInfos.getMergedFieldInfos(indexReader);
            for (final FieldInfo fieldinfo : fieldinfos) {
                CollectionStatistics collection = indexSearcher.collectionStatistics(fieldinfo.name);

                System.out.printf("%-30s%-20s%-20s%-20s%-20s", fieldinfo.name, collection.docCount(), collection.maxDoc(),
                        collection.sumDocFreq(), collection.sumTotalTermFreq());
                System.out.println();
            }
        }
        //Si le pasan el -field y el campo la variable field sera sobrescrita y pasará a tener una longitud mayor de 1
        else{
            CollectionStatistics collection = indexSearcher.collectionStatistics(field);

            System.out.printf("%-30s%-20s%-20s%-20s%-20s", field, collection.docCount(), collection.maxDoc(),
                    collection.sumDocFreq(), collection.sumTotalTermFreq());
        }

        try {
        	indexReader.close();
        	dir.close();
        } 
        catch (Exception e) {
        	e.printStackTrace();
        } 
    }


}