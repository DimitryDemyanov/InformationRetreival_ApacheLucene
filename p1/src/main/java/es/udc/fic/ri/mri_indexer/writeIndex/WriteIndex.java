package es.udc.fic.ri.mri_indexer.writeIndex;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class WriteIndex {

    public static void main(String[] args) throws IOException {
    	
    	String usage = "WriteIndex:/n" 
				+ "[-index INDEX_PATH]\n"
				+ "[-outputfile DIRECTORIO_SALIDA]\n";	
		
        
        String indexPath = null; //= carpeta de un índice
        String outputPath = "writeIndex.txt"; //= archivo de salida donde se vuelcan los campos del índice
        Document doc = null;

        List<IndexableField> fields = null;

        for (int i = 0;i<args.length;i++) {
            if ("-index".contentEquals(args[i])) {
                indexPath = args[i+1];
                i++;
            } 
            else if ("-outputfile".contentEquals(args[i])) {
                outputPath = args[i+1];
                i++;
            } else if ((args.length != 1)) {
                System.out.println("Error: ");
                System.out.println(usage);
                System.exit(1);
            }
        }

        //Se comprueba de que se han metido los 2 campos
        if (indexPath == null || indexPath.length() == 0) {
            System.out.println("Error: IndexPath is mandatory");
            System.out.println(usage);
            System.exit(1);
        }
        
        if (outputPath == null || outputPath.length() == 0) {
            System.out.println("Error: OutpuPath is mandatory");
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



        try{
            FileWriter outputFichero = new FileWriter (outputPath);
            PrintWriter printW = new PrintWriter(outputFichero);

            for (int i = 0; i < indexReader.numDocs(); i++) {

                try {
                    doc = indexReader.document(i);
                } catch (CorruptIndexException e1) {
                    System.out.println("Graceful message: exception " + e1);
                    e1.printStackTrace();
                } catch (IOException e1) {
                    System.out.println("Graceful message: exception " + e1);
                    e1.printStackTrace();
                }

                printW.println("\n"+"docID " + i);

                fields = doc.getFields(); //se obtienen los campos

                for (IndexableField field : fields) { //para cada campo indexable, se obtiene el nombre del campo, y el valor de su campo
                    //de esta forma se puede acceder al valor de los campos sin saber el nombre de los campos
                    String fieldName = field.name();
                    printW.println(fieldName + ": " + doc.get(fieldName));
                }
            }
            printW.flush();
            outputFichero.close();
            printW.close();
            indexReader.close();
            dir.close();
            
            System.out.println("Indexed in " + outputPath);
            
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}