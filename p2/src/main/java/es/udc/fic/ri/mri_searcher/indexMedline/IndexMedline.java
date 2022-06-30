package es.udc.fic.ri.mri_searcher.indexMedline;


import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.similarities.*;
import org.apache.lucene.index.IndexOptions;





//import es.udc.fic.ri.mri_searcher.LectorConfigProperties;



/**
 * 
 * Se ha reutilizado codigo del IndexNPL de la practica de a.vaquero del curso anterior
 *
 */
public class IndexMedline {
	

	
	
	public static void main(String[] args) {		
		
		String usage = "IndexFiles MultiThread:/n" 
				+ "[-index INDEX_PATH]\n"
				+ "[-docs DOCS_PATH]\n"
				+ "[-openmode {CREATE | APPEND | CREATE_OR_APPEND}]\n";	
		
		String indexPath = ".\\CarpetaPruebas\\indexMedline"; //Por defecto el index se guardara en una carpeta del proyecto con este nombre
		String docsPath = null;
		String openmode = "create_or_append";
		String indexingModel = "tfidf";
		float indexingModelValue = 0;
		
		for (int i = 0;i<args.length;i++) {
			if ("-index".contentEquals(args[i])) {
				indexPath = args[i+1];
				i++;
			} else if ("-docs".contentEquals(args[i])) {
				docsPath = args[i+1];
				i++;
			} else if ("-openmode".contentEquals(args[i])) {
				openmode = args[i+1];
				i++;
			} else if ("-indexingmodel".contentEquals(args[i])) {
				indexingModel = args[i+1];
				
				//Cogemos el valor lambda
				if (indexingModel.toLowerCase().contentEquals("jm")) {
					indexingModelValue = esLambdaValido(args[i+2]);
					i++;
				}
				
				i++;
			} else if ((args.length != 1) | ("-help".contentEquals(args[i]))) {
				System.err.println("Error: ");
				System.out.println(usage);
				System.exit(1);
			}
		}
		
		//Pruebas entrada
		/**
		System.out.println(indexPath);
		System.out.println(docsPath);
		System.out.println(openmode);
		System.out.println(indexingModel);
		System.out.println(indexingModelValue);
		
		//System.exit(1);
		
		
		/*Leer valores doc y indexingmodel del config.properties*/
		
		
		if (docsPath == null) {
			System.err.println("Usage: " + "Missing doc path");
			System.exit(1);
		}

		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" + docDir.toAbsolutePath()
					+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}
		
		
		
		
		
	
		
		/**Ejecución**/

		Date start = new Date();
		try {
			System.out.println();
			System.out.println("Indexing to directory '" + indexPath + "'...");

			Directory indexDir = FSDirectory.open(Paths.get(indexPath));
			StandardAnalyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);	
			
			//Configuramos el indexWriter
			setOpenmode(iwc, openmode); 
			setIndexModel(iwc, indexingModel, indexingModelValue);
			
			IndexWriter writer = new IndexWriter(indexDir, iwc);
			
			//Solo hace falta indexar el documento "doc-text"
			indexDocs(writer, docDir);
		
			System.out.println();
			System.out.println("Finished.");
			
			writer.commit();
			writer.close();
			
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() +  " total milliseconds");
			
		}catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}
	
	/**OJOOOOOOOO. PENSAR SI SALTARSE ESTO O NO**/
	
	/*Metodo tal cual en el ejemplo "IndexFiles.java"*/
	static void indexDocs(final IndexWriter writer, Path path) throws IOException {
		//System.out.println("EJECUTANDOSE INDEXDOCS");
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
						indexDoc(writer, file);
					} catch (IOException ignore) {
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			indexDoc(writer, path);
		}
	}

	static void indexDoc(IndexWriter writer, Path file) throws IOException {
		
		//try (InputStream stream = Files.newInputStream(file)) {
		try (FileInputStream stream = new FileInputStream(file.toString())){

			BufferedReader contenidoMedline = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			//int nLineasArchivo = Math.toIntExact(contenidoArchivo.lines().count());
			Map<Integer,String> contenidoDoc = new HashMap<>();

			//ArrayList<String> listaContents = new ArrayList();
			StringBuilder contenidoDocActual = new StringBuilder();
			String linea = "";
			int docID = 0;
			boolean firstID = true;
			
			
			int iteracion=0; 
			
			
			System.out.println();
			System.out.println("Running...");
			
			//Parseamos doc-text
			while ((linea = contenidoMedline.readLine()) != null) {
				
				//System.out.println(linea);
				
				
				
				if(linea.contains(".I ")) {
				//System.out.println(linea.substring(0,3));
				
				//if(linea.substring(0,3).equals(".I ")) {
					
					
					if (!firstID) {
						
						//System.out.println("Entra para almacenarse");
						
						//System.out.println(contenidoDocActual.toString());
						
						
						
						
						contenidoDoc.put(docID, contenidoDocActual.toString());
						contenidoDocActual = new StringBuilder();
						
					}
					firstID = false;
					docID = Integer.parseInt(linea.substring(3));
					contenidoMedline.readLine(); //Nos saltamos la linea con ".W"
					
				} else {
					contenidoDocActual.append(linea + " ");
					//contenidoDocActual.append(linea);
					//contenidoDocActual.append(" ");
				}	
			}
			
			//Ultimo documento
			contenidoDoc.put(docID, contenidoDocActual.toString());
			
			contenidoMedline.close();
			
			/*
			for (int i = 0; i < listaContents.size(); i++) {
				Document doc = new Document();
				
				doc.add(new Field("DocIDNPL", String.valueOf(i+1), CAMPO_ALMACENADO));
				doc.add(new Field("Contents", listaContents.get(i), CAMPO_ALMACENADO));
	
				if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
					writer.addDocument(doc);
				} else {
					writer.updateDocument(new Term("path", file.toString()), doc);
				}
			}*/
			
			
			
			
			
			//int iteracion=0; 
			
			
			for (Map.Entry<Integer, String> entry : contenidoDoc.entrySet()) {
				
				
				iteracion++;
				
				Document doc = new Document();
				
				String nuevoDocID = entry.getKey().toString();
				String nuevoContentDoc = entry.getValue();
				
				//iteracion++; 
				//System.out.println(nuevoDocID);
				//System.out.println(nuevoContentDoc);
				
				
				
				
				FieldType type_stored = new FieldType();
				type_stored.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
				type_stored.setTokenized(true);
				type_stored.setStored(true);
				type_stored.setStoreTermVectors(true);
				type_stored.setStoreTermVectorPositions(true);
				type_stored.freeze();
				
				
				
				
				
				//Campo "DocIDMedline"
				//doc.add(new Field("DocIDMedline", entry.getKey().toString(), newFieldType()));
				doc.add(new Field("DocIDMedline", nuevoDocID, type_stored));
				
				
				
				
				
				
				//Campo "Contents"
				
				
				
				
				
				doc.add(new Field("Contents", nuevoContentDoc, type_stored));
				
				// CREO QUE NO ES NECESARIO PORQUE EL PRINTEO REALENTIZA
				if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
					System.out.println("adding " + nuevoDocID);
					writer.addDocument(doc);
				} else {
					System.out.println("updating " + entry.getKey().toString());
					writer.updateDocument(new Term("path", file.toString()), doc); ///NOOO SE ACTUALIZA
				}
				
				
				//writer.addDocument(doc);
				
			}//End for
			
			System.out.println("Iteracion: "+iteracion);
			
			
		}
	}
	
	/*private static FieldType newFieldType() {

		final FieldType type_stored = new FieldType(StringField.TYPE_STORED);

		type_stored.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		type_stored.setTokenized(true);
		type_stored.setStored(true);
		type_stored.setStoreTermVectors(true);
		type_stored.setStoreTermVectorPositions(true);
		type_stored.freeze();

		return type_stored;
	}*/
	
	
	/**Otros metodos**/
	private static boolean isDocID(String cadena){
		try {
			Integer.parseInt(cadena);
			return true;
		} catch (NumberFormatException nfe){
			return false;
		}
	}

	
	
	//Metodo para configurar el IndexWriter
	private static void setOpenmode(IndexWriterConfig iwc, String openmode) {
		
		if (openmode.toLowerCase().contentEquals("create")) {	
			iwc.setOpenMode(OpenMode.CREATE);
			System.out.println("OPENMODE: CREATE");
			
		} else if (openmode.toLowerCase().contentEquals("create_or_append")) {
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			System.out.println("OPENMODE: CREATE_OR_APPEND");
			
		} else if (openmode.toLowerCase().contentEquals("append")) {
			iwc.setOpenMode(OpenMode.APPEND);
			System.out.println("OPENMODE: APPEND");
		} 
	}
	
	private static void setIndexModel(IndexWriterConfig iwc, String indexmodel, float valor) {
		
		if (indexmodel.toLowerCase().contentEquals("jm")) {	
			iwc.setSimilarity(new LMJelinekMercerSimilarity(valor));
			System.out.println("SIMILARITY: JM");
			
		} else if (indexmodel.toLowerCase().contentEquals("tfidf")) {
			iwc.setSimilarity(new ClassicSimilarity());
			System.out.println("SIMILARITY: TFIDF");
		} else {
			System.err.println("ERROR: Tipo de indexingModel no identificado");
			System.exit(1);
		}
	}
	
	
	
	//Metodo para comprobar si el valor de lambda es valido
	private static float esLambdaValido(String argumento) {
		
		float varSalida = 0;
		
		try { //Comprobamos si es un numero
			
			varSalida = Float.parseFloat(argumento);
			if ((varSalida<0)||(varSalida>1)) {
				System.err.println("ERROR: El valor lambda especificado tiene que estar dentro del rango [0..1]");
				System.exit(1);
			}//Si es un 0 o un número negativo se devuelve un error	
	    }
	    catch (NumberFormatException e){
	        System.err.println("ERROR: El valor de lambda no contiene un numero de entrada");
	        System.exit(1);;
	    }
		
		System.out.println("Valor de lambda especificado: "+ varSalida);
		return varSalida;
	}
}


