package es.udc.fic.ri.mri_indexer.indexFiles;



import java.io.IOException;
import java.io.File;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;


import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import es.udc.fic.ri.mri_indexer.LectorConfigProperties;




/**
 * En toda la clase de indexFiles se ha reutilizado algunas partes del codigo de la practica de @author a.vaquero del año pasado
 *
 */
public class IndexFiles {
	

	
	public static void main(String[] args) {		
		
		String usage = "IndexFiles MultiThread:/n" 
				+ "[-index INDEX_PATH]\n"
				+ "[-docs DOCS_PATH]\n"
				+ "[-openmode {CREATE | APPEND | CREATE_OR_APPEND}]\n"
				+ "[-numThreads NUMBER_OF_THREADS]\n"
				+ "[-create]\n"
				+ "[-update]\n"
				+ "[-partialIndexes]\n"
				+ "[-deep NUMBER_DEPTH]\n"
				+ "[-onlyFiles]\n"
				+ "[-onlyTopLines]\n"
				+ "[-onlyBottomLines]\n"
				+ "Se indexan los documentos de los paths especificados en el apartado 'docs' del archivo 'config.properties', "
				+ "creando un indice de Lucene en el path especificado en INDEX_PATH";	
		
		String indexPath = "default_indexPath"; //IndexPath por defecto
		String openmode = "create";
		String docsPath = null;
		String partialIndexPath = "";
		int nThreads = Runtime.getRuntime().availableProcessors();
		System.out.println("numThreads por defecto: "+nThreads);
		int nDeep = 0;
		boolean deepFlag = false;
		boolean updateFlag = false;
		boolean createFlag = false;
		boolean onlyFiles = false;
		boolean partialIndexFlag = false;
		boolean onlyTopLines = false;
		boolean onlyBottomLines = false;
		int numberTopLines = -1;
		int numberBottomLines = -1;
		
		final double RAM = 512.0;
		
		
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
			} else if ("-numThreads".contentEquals(args[i])) {
				nThreads = esNumeroValido(args[i+1], "-numThreads"); //Comprueba que haya recibido un numero positivo por pantalla
				i++;
			} else if ("-create".equals(args[i])) {
				createFlag = true;
				openmode = "create";
			} else if ("-update".equals(args[i])) {
				updateFlag = true;
				openmode = "append";
			} else if ("-onlyFiles".contentEquals(args[i])) {
				onlyFiles = true;
			} else if ("-partialIndexes".contentEquals(args[i])) {
				partialIndexFlag = true;
			} else if ("-deep".contentEquals(args[i])) {
				nDeep = esNumeroValido(args[i+1], "-deep"); //Comprueba que haya recibido un numero positivo por pantalla
				deepFlag = true;
				i++;
			} else if ("-onlyTopLines".contentEquals(args[i])) {
				onlyTopLines = true;
			} else if ("-onlyBottomLines".contentEquals(args[i])) {
				onlyBottomLines = true;
			} else if ((args.length != 1) | ("-help".contentEquals(args[i]))) {
				System.out.println("Error: ");
				System.out.println(usage);
				System.exit(1);
			}
		}
		
		
		
		
		/**Analisis de entrada y comprobacion de errores**/
		
		//Se necesita un path de los documentos a indexar
		if (docsPath == null) {
			System.out.println("ERROR: No se ha especificado el \"path\" del directorio a indexar correctamente");
			System.exit(1);
		}
		
		//Se devuelve un error si ha recogido un -numThreads 0
		if (nThreads == 0) {
			System.out.println("ERROR: Se ha especificado numThreads = 0, por lo que no se puede ejecutar el programa");
			System.exit(1);
		}
		
		//Si se seleccionan ambas opciones, se hará create_or_append
		if ((updateFlag && createFlag) 
				|| (openmode.toLowerCase().contentEquals("append") && createFlag) 
				|| (openmode.toLowerCase().contentEquals("create") && updateFlag)) {
			openmode = "create_or_append";		
		}
		
		
		//No se podra ejecutar la opcion "-deep" y "-partialIndex" al mismo tiempo
		if (deepFlag && partialIndexFlag) {
			System.out.println("ERROR: No se puede activar el partialIndex y el nDeep al mismo tiempo");
			System.exit(1);
		}
		
		
		
		
		
		
		/** Recoger paths carpetas de primer nivel y crear los paths de los indices paciales **/
		
		if (partialIndexFlag) {
			
			//Leemos las carpetas que se encuentran en el primer nivel
			
			File carpetaDocsPath = new File(docsPath);
			StringBuilder docsPathBuilder = new StringBuilder();
			StringBuilder partialIndexBuilder = new StringBuilder();
			
			for (final File ficheroEntrada : carpetaDocsPath.listFiles()) {

		        if (ficheroEntrada.isDirectory()) { /** (char)92 = "/" **/
		        	docsPathBuilder.append(docsPath + (char)92 + ficheroEntrada.getName() + " ");
		        	
		        	//Añadir subcarpeta con sufijo
		        	partialIndexBuilder.append(indexPath + (char)92 + ficheroEntrada.getName() + "_partial ");
		        }
			}
			
			//Guardamos el nuevo docsPath
			docsPath = docsPathBuilder.toString();
			
			//Guardamos el path de los indices parciales
			partialIndexPath =  partialIndexBuilder.toString();
	
		}
		
		
		
		
		/** Recolectar el path por profundidad **/
		
		/**
		 * NOTA A TENER EN CUENTA:
		 * Se ha realizado esta parte del codigo debido a que la funcion Files.walkFileTree
		 * de la linea 385 no realiza el funcionamiento esperado.
		 * 
		 * Siendo conscientes de que la mejor manera seria emplear esa funcion Files.walkFileTree, 
		 * debido al problema desconocido hemos implementado la lectura de carpetas en profundidad
		 * de la siguiente manera.
		 * 
		 */
		
		if (deepFlag) {
			
			if (nDeep == 0) {
				System.out.println("Numero de profundidad a indexar = 0 ---> No se indexa nada");
				System.exit(1);
			}
			
			//Creamos un StringBuilder que almacenara el docPath a indexar
			StringBuilder docsPathBuilder = new StringBuilder();
			
			for(int n = 0; n < nDeep; n++) {
				
				//Guardamos en un array cada path que contenga el string docsPath
				String[] deepAnalisisDocsPath = docsPath.split(" ");
				docsPathBuilder.setLength(0);//Borrado del StringBuilder
				docsPath = "";
				int i;
				
				for(i = 0; i < deepAnalisisDocsPath.length; i++) {
					
					File carpetaDocsPath = new File(deepAnalisisDocsPath[i]);
					
					for (final File ficheroEntrada : carpetaDocsPath.listFiles()) {
				        if (ficheroEntrada.isDirectory()) {
				        	docsPathBuilder.append(deepAnalisisDocsPath[i] + (char)92 + ficheroEntrada.getName() + " "); 	
				        }
					}
				}
				
				//Guardamos el nuevo docsPath
				docsPath = docsPathBuilder.toString();			
			}

		}
		
		
		
		
		
		
		//Comprobamos directorios a indexar, y los guardamos en una lista
		List<Path> listaDocsPath = new ArrayList<>();		
		
		//Comprobamos si el directorio existe
		for (String stringPath : docsPath.split(" ")) {
			Path docPath = Paths.get(stringPath);
			if (!Files.isReadable(docPath)) {
				System.out.println("Document directory '" 
						+ docPath.toAbsolutePath()
						+ "' does not exist or is not readable, please check the path");
				System.exit(1);
			}
			listaDocsPath.add(docPath);	
		}
		
		
		
		
		//Comprobamos las entradas de onlyTopLines y onlyBottomLines
		if(onlyTopLines) {
			String StringTopLines = leerParametro("onlyTopLines");
			if(!StringTopLines.isEmpty()) 
				numberTopLines = esNumeroValido(StringTopLines, "-onlyTopLines");
		}
		
		
		if(onlyBottomLines) {	
			String StringBotomLines = leerParametro("onlyBottomLines");
			if(!StringBotomLines.isEmpty()) 
				numberBottomLines = esNumeroValido(StringBotomLines, "-onlyBottomLines");
		}

		
		/**Ejecución**/
		Date start = new Date();
		try {
			System.out.println();
			System.out.println("Indexing to directory '" + indexPath + "'...");
			
			Directory indexDir;
			IndexWriter writerIndexFiles = null;
			
			Directory[] partialDirectories = null;
			List<IndexWriter> ListPartialIndexesWriter = new ArrayList<>();
			
			if(!partialIndexFlag) { //Indexacion normal
				
				indexDir = FSDirectory.open(Paths.get(indexPath));
				StandardAnalyzer analyzer = new StandardAnalyzer();
				IndexWriterConfig iwc = new IndexWriterConfig(analyzer);	
				setOpenmode(iwc, openmode); 
				
				iwc.setRAMBufferSizeMB(RAM);
				writerIndexFiles = new IndexWriter(indexDir, iwc);
			
			
			}else { //PartialIndex
				
				String[] dirs = partialIndexPath.split(" ");
						
				partialDirectories = new Directory[dirs.length];
				
				for (int i=0; i<dirs.length; i++) {
					Directory partialIndexdir;
					try {
						partialIndexdir = FSDirectory.open(Paths.get(dirs[i]));
						StandardAnalyzer analyzerPartialIndex = new StandardAnalyzer();
						IndexWriterConfig indexWriterConfigPartialIndex = new IndexWriterConfig(analyzerPartialIndex);
						setOpenmode(indexWriterConfigPartialIndex, openmode);
						indexWriterConfigPartialIndex.setRAMBufferSizeMB(RAM);
						
						ListPartialIndexesWriter.add(new IndexWriter(partialIndexdir, indexWriterConfigPartialIndex));
						partialDirectories[i] = partialIndexdir;

					} catch (IOException e) {
						e.printStackTrace();
					}	
				}	
			}

			IndexWriter writerThread = null;
			final ExecutorService executor = Executors.newFixedThreadPool(nThreads);	
			
		
			
			for (int i=0; i < listaDocsPath.size(); i++) {
				
				if (partialIndexFlag) {
					writerThread = ListPartialIndexesWriter.get(i);
				} else {
					writerThread = writerIndexFiles;	
				}
				indexDocs(writerThread, listaDocsPath.get(i), nDeep, numberTopLines, numberBottomLines, onlyFiles, executor);
			}
		
			
			
			
			
			/** Cerramos todos los threads activos **/
			
			executor.shutdown(); 
			
			/* Wait up to 1 hour to finish all the previously submitted jobs */
			try {
				executor.awaitTermination(1, TimeUnit.HOURS);
			} catch (final InterruptedException e) {
				e.printStackTrace();
				System.exit(-2);
			}
			
			System.out.println();
			System.out.println("Finished all threads");
			
			if (partialIndexFlag) {
				// Cerramos todos los writers de los partialIndexes
				for (IndexWriter closeWrt : ListPartialIndexesWriter) {
					closeWrt.commit();
					closeWrt.close();
				}
					
			} else {
				writerIndexFiles.commit();
				writerIndexFiles.close();
			}		
			
			
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() +  " total milliseconds");
			
			
		}catch (IOException e) {
			System.out.println(e.getMessage());
		}
	
	}// Fin del main() de indexFiles
	

	
	
	static void indexDocs(final IndexWriter writer, Path path, int nDeep, int numberTopLines, int numberBottomLines, boolean onlyFiles, ExecutorService executor) throws IOException {
		if (Files.isDirectory(path)) {
			
			/**ATENCION: Esta funcion cuyo funcionamiento no es el esperado**/
			Files.walkFileTree(path, new HashSet <>(), nDeep, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
					ManagerIndexThread managerIndexThread = new ManagerIndexThread(file, writer, numberTopLines, numberBottomLines);
					managerIndexThread.start(onlyFiles, executor);
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			System.out.println("path Texto: "+ path);
			ManagerIndexThread manIndexThread = new ManagerIndexThread(path, writer, numberTopLines, numberBottomLines);
			manIndexThread.start(onlyFiles, executor);
		}
	}
	
	
	
	/** Metodos implementados para la correcta lectura de los parametros de entrada**/
	
	private static String leerParametro(String parametro) {
		return LectorConfigProperties.getParametro(parametro);
	}
	
	
	//Metodo para comprobar que el parametro de entrada es un numero positivo o 0
	private static int esNumeroValido(String argumento, String tipoArgumento) {
		
		int varSalida = 0;
		
		try { //Comprobamos si es un numero
			
			varSalida = Integer.parseInt(argumento);
			if (varSalida<0) {
				System.err.println("ERROR: El numero de "+ tipoArgumento +" especificado tiene que ser mayor que 0");
				System.exit(1);
			}//Si es un 0 o un número negativo se devuelve un error
			
	    }
	    catch (NumberFormatException e){
	        System.out.println("ERROR: "+ tipoArgumento +" no contiene un numero de entrada");
	        System.exit(1);;
	    }
		
		System.out.println("Numero de "+ tipoArgumento +" especificado: "+ varSalida);
		return varSalida;
	}
	

	
	//Metodo para configurar el IndexWriter
	private static void setOpenmode(IndexWriterConfig iwc, String openmode) {
		
		if (openmode.toLowerCase().contentEquals("create")) {	
			iwc.setOpenMode(OpenMode.CREATE);
			//System.out.println("OPENMODE: CREATE");
			
		} else if (openmode.toLowerCase().contentEquals("create_or_append")) {
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			//System.out.println("OPENMODE: CREATE_OR_APPEND");
			
		} else if (openmode.toLowerCase().contentEquals("append")) {
			iwc.setOpenMode(OpenMode.APPEND);
			//System.out.println("OPENMODE: APPEND");
		} 
	
	}
	

}