package es.udc.fic.ri.mri_searcher.searchEvalMedline;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
//import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
//import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

//import es.udc.fi.ri.mri_searcher.Util;






public class SearchEvalMedline {
	
	
	public static void main(String[] args) throws IOException{

		
		String usage = "SearchEvalMedline:\n"
				+ "[-search jm lambda | tfidf]\n"
				+ "[-indexin INDEX_PATH]\n"
				+ "[-cut n]\n"
				+ "[-top m]\n"
				+ "[-queries all | int1 | int1-2]";
		
		String searchArg = "";
		String indexPath = "";
		String queryRange = "";
		
		float lambdaJmValue = 0;
		int cutN = 0;
		int topM =  0;
		
		Similarity similarity = null;
		
		
		Properties prop = new Properties();
        prop.load(new FileReader(".\\src\\main\\resources\\config.properties"));
        String queriesPath = null;
        queriesPath = prop.getProperty("queriesPath");
        
        //Variables para nombres de fichero
        //String txtFileName = "medline";
        //String csvFileName = "medline";
        String firstPartFileName = "medline";
        String secondPartFileName = "";
        
        //System.out.println("COMIENZA A PASAR POR EL MAIN");
		
		
		//////*****PLANTEARSE SI HACER MAS COMPROBACIONES DE QUE SI CADA OPCION TIENE DOS ARGUMENTOS*******///////
		for (int i = 0;i<args.length;i++) {
			if ("-search".contentEquals(args[i])) {
				searchArg = args[i+1];
				
				//Cogemos el valor lambda
				
				if(searchArg.toLowerCase().contentEquals("tfidf")) {
					similarity = new ClassicSimilarity();
					//firstPartFileName = firstPartFileName +"";
					
				} else if (searchArg.toLowerCase().contentEquals("jm")) {
					lambdaJmValue = esLambdaValido(args[i+2]);
					
					
					similarity = new LMJelinekMercerSimilarity(lambdaJmValue);
					//similarity = new LMJelinekMercerSimilarity((float)0.001); PREGUNTAR A MAYORGA SOBRE COMO HACER SUAVIZACION 0.0 sin esta funcion no me lo permite
					
					secondPartFileName = ".lambda." + args[i+2];
					i++;
				}
				
				firstPartFileName = firstPartFileName + "." + searchArg.toLowerCase();
				
				//System.out.println("similarity : "+ similarity );
				
				i++;
			} else if ("-indexin".contentEquals(args[i])) {
				indexPath = args[i+1];
				//System.out.println("indexPath : "+ indexPath );
				i++;
			} else if ("-cut".contentEquals(args[i])) {
				cutN = esNumeroValido(args[i+1], args[i]);
				//System.out.println("similarity : "+ cutN );
				i++;
			} else if ("-top".contentEquals(args[i])) {
				topM = esNumeroValido(args[i+1], args[i]);
				//System.out.println("topM : "+ topM );
				
				
				i++;
				
			} else if ("-queries".contentEquals(args[i])) {
				queryRange = args[i+1];
				//System.out.println("queryRange : "+ queryRange );
				i++;
				
			} else if ((args.length != 1) | ("-help".contentEquals(args[i]))) {
				System.err.println("Error. Argumento no reconocido: ");
				System.out.println(usage);
				System.exit(1);
			}
		}
		
		//System.out.println("SALE DEL MAIN " );
		//System.out.println();
		//System.exit(1);
		
		/** Comprobacion de errores **/
		
		//Modelo de busqueda	
		if (searchArg.contentEquals("")) {
			System.err.println("ERROR: Se necesita especificar el modelo de busqueda [-search jm lambda | tfidf ]");
			System.out.println(usage);
			System.exit(1);
		}
		
		
		
		if (searchArg == "") {
			System.err.println("ERROR: No se ha especificado el modelo de similaridad correctamente");
			System.out.println(usage);
			System.exit(1);
		}
		
		if (indexPath == "") {
			System.err.println("ERROR: No se ha especificado el  \"index_path\"  a usar");
			System.out.println(usage);
			System.exit(1);
		}
		
		
		//Error argumento "queries"... continuar por linea 127 del de angela
		
		//String[] ranges;
		
		if (queryRange == "") {
			System.out.println("ERROR: No se ha especificado correctamente el argumento \"-queries\"");
			System.out.println(usage);
			System.exit(1);
		} //else {
			//ranges = queryRange.split("-");
			/*if (ranges.length == 2) {
				
			}*/
			
		//}
		
		String[] ranges = queryRange.split("-");
		
		
		
	
		
		
		
		
		
		
		try {
			
			//System.out.println("Comienza el try.");

			Map<Integer, String> textoMedlineQuery = parseQueries(Paths.get(queriesPath+"MED.QRY"));
			Map<Integer, String> mapForSearch = new HashMap<Integer, String>();

			/**
			 * En este bucle for se eliminan los parentesis de las querys ya que
			 * algunas contienen un uso "ilegal" de la jerarquia de los parentesis
			 * a la hora de parsear. Concretamente la query 29.
			 */
	        for(Integer r=1; r <= textoMedlineQuery.size();r++){
	            String queryText = textoMedlineQuery.get(r);
	            queryText = queryText.replaceAll("[(]", "");
	            queryText = queryText.replaceAll("[)]", "");
	            textoMedlineQuery.put(r,queryText);
	        }
			
	        
			if (ranges.length == 2) {
				//System.out.println("ENTRA EN INT1-INT2");
				int inicio = esNumeroValido(ranges[0],"-queries int1");
				int fin = esNumeroValido(ranges[1],"-queries int2");
				
				//Si esta puesto al revés, lo cambiamos de orden
				if (inicio > fin) { 
					int aux = fin;
					fin = inicio;
					inicio = aux;
				}
				for (int i = inicio; i <= fin; i++) {
					mapForSearch.put(i, textoMedlineQuery.get(i));
				}
				
				secondPartFileName = secondPartFileName + ".q" + ranges[0] + "-" + ranges[1];
				
			} else {
				if (ranges[0].contentEquals("all")) {
					//System.out.println("ENTRA EN ALL");
					mapForSearch.putAll(textoMedlineQuery);
					secondPartFileName = secondPartFileName + ".qall";
				} else { //caso de solo int1
					//System.out.println("ENTRA EN INT1");
					mapForSearch.put(esNumeroValido(ranges[0],"-queries int1"), textoMedlineQuery.get(Integer.parseInt(ranges[0])));
					secondPartFileName = secondPartFileName + ".q" + ranges[0];
				}
			}
			
			/*
			System.out.println();
			System.out.println("mapForSearch :"+mapForSearch);
			System.out.println("similarity: "+similarity);
			System.out.println("indexPath: "+indexPath);
			System.out.println("cutN: "+cutN);
			System.out.println("topM: "+topM);
			System.out.println("queriesPath: "+queriesPath);
			*/
			
			//System.out.println("FIIIIN ANTES DEL SEARCH PAGE");
			//System.exit(1);
			
		
			

			//Util.searchQuery(mapForSearch, similarity, indexPath, cut, top, metrica, new ArrayList<Integer>());
			doSearchQuery(mapForSearch, similarity, indexPath, cutN, topM, queriesPath, firstPartFileName, secondPartFileName);
			
			/*
			if (ints[0].equals("0")) {
				enUso.putAll(text);
			} else if (ints.length == 1) {
				enUso.put(Integer.parseInt(ints[0]), text.get(Integer.parseInt(ints[0])));
			} else {
				int inicio = Integer.parseInt(ints[0]);
				int fin = Integer.parseInt(ints[1]);
				if (inicio > fin) { //Si esta puesto al revés, lo reordenamos
					int aux = fin;
					fin = inicio;
					inicio = aux;
				}
				for (int i = inicio; i <= fin; i++) {
					enUso.put(i, text.get(i));
				}
			}*/
			
			
		} catch (NumberFormatException | IOException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	} // End Main
	
	
	
	/** doSearchQuery **/
	/*public static Map<Integer, Float> searchQuery(Map<Integer, String> enUso, Similarity similarity, String indexPath,
			int cut, int top, String metrica, List<Integer> residualList)
			throws NumberFormatException, IOException, ParseException {*/
	
	public static void doSearchQuery(Map<Integer, String> queryMapSearch, Similarity similarity, String indexPath,
			int cut, int top, String queriesPath, String firstPartFileName, String secondPartFileName)
			throws
			//NumberFormatException,
			IOException
			, ParseException
	{

		Map<Integer, List<Integer>> relevanceMedlineMap = parseRel(Paths.get(queriesPath+"MED.REL"));
		Map<Integer, Float> docsRelevRank = new HashMap<Integer, Float>();
		
		//System.out.println("1");
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
		IndexSearcher searcher = new IndexSearcher(reader);
		
		//System.out.println("2");
		Analyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser("Contents", analyzer);
		DecimalFormat logFormat = new DecimalFormat("#0.00");//MIRAAAR AQUIII
		
		//float mediaMet = 0;
		//Metricas acumuladas para luego hacer la media
		float pValueAcum = 0;
		float rValueAcum = 0;
		float apValueAcum = 0;
		
		//Contador de palabras
		int limiteWordsLinea = 12;
		int countWords;
		
		//Nombres de los ficheros
		String txtNameFile = firstPartFileName + "." + Integer.toString(top).toString() + ".hits" + secondPartFileName + ".txt";
		String csvNameFile = firstPartFileName + "." + Integer.toString(cut).toString() + ".cut" + secondPartFileName + ".csv";
		
		
		//Comprobacion nombre de archivos
		System.out.println("txtNameFile: "+txtNameFile);
		System.out.println("csvNameFile: "+csvNameFile);
		
		
		
		System.exit(1);
		
		searcher.setSimilarity(similarity);
		
		//System.out.println("3");
		
		//ELIMINABLE?
		List<Integer> residualList = new ArrayList<Integer>();
		
		//System.out.println("ANTES DEL FOR");
		
		
		
		/**
		 * A partir de aqui procedemos a las siguientes acciones:
		 * 1. Calculo de metricas
		 * 2. Imprimir resultados por pantalla ("console")
		 * 3. Crear los archivos .txt y .csv con los resultados correspondientes
		 */
		try {
			
			//Inicializamos punteros de escritura para escribir en los archivos
			PrintWriter pwtxt = new PrintWriter(new File(txtNameFile));
            StringBuilder sbtxt = new StringBuilder();
            
            PrintWriter pwcsv = new PrintWriter(new File(csvNameFile));
            StringBuilder sbcsv = new StringBuilder();
			
            
            
            //CONTINUAR POR AQUIIII
            
            
            

			for (Map.Entry<Integer, String> querySearching : queryMapSearch.entrySet()) {
				
				
				System.out.println();
				
				System.out.println("######################## Nº Query: " + querySearching.getKey() + " ########################");
				
				
				//System.out.println("Contenido: " + querySearching.getValue() + "\n");
				
				
				
				String[] separadorWordsQuery = querySearching.getValue().split(" ");
				
				System.out.println("### Contenido query:");
				
				countWords = 0;
				
				for(Integer r=0; r < separadorWordsQuery.length ;r++){
					
					System.out.print(separadorWordsQuery[r].toString() + " ");
					countWords++;
					
					if (countWords >= limiteWordsLinea) {
						//System.out.println();
						System.out.print("\n");
						countWords = 0;
					}
				}
				
				System.out.println();
				System.out.println();
				
				
				
				
				
				
				Query query = parser.parse(querySearching.getValue());
				
				//Guardamos en una lista los DocIDMedline de los docs relevantes
				List<Integer> listaRelevantes = relevanceMedlineMap.get(querySearching.getKey()); // Relevant's docsID
				
				//System.out.println("querySearching.getValue(): "+querySearching.getValue());
				
				/****AQUI FALLA EN LA QUERY 29****/
				
				
				//System.out.println("5");
	//			System.out.println("QUERYY: \n" + query);
				
				//Numero de documentos relevantes de la lista obtenida
				int totalDocsRelevantes = listaRelevantes.size();
	
				int topHits = Math.max(top, cut);
	
				TopDocs topDocs = searcher.search(query, topHits);
				ScoreDoc[] scoreDoc = topDocs.scoreDocs;
				
				int posRanking = 1;
				int docsRelvRecuperados = 0;
				float averagePrecision = 0;
				int corte = (int) Math.min(topHits, topDocs.totalHits.value);
				
				
				
				
				
				System.out.println("\n############### Ranking ###############\n");
				
				
				/*****IMPORTANTE ENTENDER EL BUCLEEE Y HACER ALGUN CAMBIO******/
				for (int i = 0; i < corte; i++) {
					
					float score = scoreDoc[i].score;
					int docIDactual = scoreDoc[i].doc;
					int docIDMedline = Integer.parseInt(reader.document(docIDactual).get("DocIDMedline"));
					
					
					
					
	
					if (i < top) { /**HAY QUE CAMBIAR A PARTIR DE AQUI**/
						
						System.out.println("## Ranking position: " + posRanking);
						System.out.println("# DocIDMedline: " + docIDMedline);
						
						System.out.println("# Score: " + logFormat.format(score));
						
						
						//El que se sabe que funciona
						//System.out.println("Contents: " + reader.document(docIDactual).get("Contents"));
						
						
						//Intentando mejorar la representacion del contenido
						
						//Primer intento
						/*
						String contentsField = reader.document(docIDactual).get("Contents");
						String[] separadorLineasContents = contentsField.split("   ");
						
						
						System.out.println("Contents:\n ");
						
						for(Integer r=0; r < separadorLineasContents.length ;r++){
							System.out.println(separadorLineasContents[r].toString());
						}
						*/
						
						
						//Segundo intento
						
						String contentsField = reader.document(docIDactual).get("Contents");
						String[] separadorWordsContents = contentsField.split(" ");
						
						
						System.out.print("\n# Contents: \n -> ");
						
						//System.out.println("Nº Words: "+separadorWordsContents.length);
						
						countWords = 0;
						
						for(Integer r=0; r < separadorWordsContents.length ;r++){
							if (!separadorWordsContents[r].toString().contentEquals(" ")) {
								System.out.print(separadorWordsContents[r].toString() + " ");
								countWords++;
							}
							
							
							if (countWords >= limiteWordsLinea) {
								//System.out.println();
								System.out.print("\n");
								countWords = 0;
							}
						}
						
						System.out.println();
						System.out.println();
						
						
					}
					if (listaRelevantes.contains(docIDMedline)) {
						if (residualList.isEmpty() || (!residualList.isEmpty() && !residualList.contains(docIDMedline))) {
							if (i < top) {
								System.out.println("# ¿Relevante?: SI");
								System.out.println("------------------------------------------------------------------");
							}
							if (i < cut) {
								docsRelvRecuperados++;
								//int dem = i + 1;
								averagePrecision += (float) docsRelvRecuperados / (i + 1);
								docsRelevRank.put(docIDMedline, score);
							}
						} else {
							if (i < top) {
								System.out.println("# ¿Relevante?: NO");
								System.out.println("------------------------------------------------------------------");
							}
						}
					} else {
						if (i < top) {
							System.out.println("# ¿Relevante?: NO");
							System.out.println("------------------------------------------------------------------");
						}
					}
					if (i < top) {
						System.out.println();
					}
					
					//Incrementamos posicion de ranking
					posRanking++;
	
				}
				
				//Valores metricas query
				float valorP = 0;
				float valorR = 0;
				float valorAP = 0;
				
				System.out.println("## Valores de metricas en query:");
				
				valorP = (float) docsRelvRecuperados / cut;
				System.out.println("P@n: " + valorP);
				//} else if (metrica.equals("R")) {
				valorR = (float) docsRelvRecuperados / totalDocsRelevantes;
				System.out.println("R@n: " + valorR);
				//} else if (metrica.equals("MAP")) {
				valorAP = (float) averagePrecision / totalDocsRelevantes;
				System.out.println("AP@n: " + valorAP);
				//}
				
				
				//Acumulamos las metricas
				pValueAcum += valorP;
				rValueAcum += valorR;
				apValueAcum += valorAP;
				
				
				System.out.println();
				
			}//End For Map
			
			System.out.println("*************************************\n");
			System.out.println("Metrica promedio de todas las querys:");
			
			
			int tamQueryMap = queryMapSearch.size();
			
			float mediaP = (float) pValueAcum / tamQueryMap;
			float mediaR = (float) rValueAcum / tamQueryMap;
			float mediaAP = (float) apValueAcum / tamQueryMap;
	
			//if (metrica.equals("P")) {
			System.out.println("P@n: " + mediaP);
			//} else if (metrica.equals("R")) {
			System.out.println("R@n: " + mediaR);
			//} else if (metrica.equals("MAP")) {
			System.out.println("MAP@n: " + mediaAP);
			//}
				
			//No hace falta
			//docsRelevRank.put(-1, media);
		
		} catch (Exception e){
            e.printStackTrace();
        }

		reader.close();
		//return docsRelevRank;
	} //end doSearchQuery
	
	
	
	
	
	
	
	
	/**Metodos importantes PENDIENTE DE SI MOVERLO A OTRA CLASE O NO**/
	
	//Lectura MED.QRY
	 public static Map<Integer,String> parseQueries(Path file) 
			 throws
			 //FileNotFoundException,
			 IOException
	 {
	        Map<Integer, String> result = new HashMap<>();
	        FileInputStream stream = new FileInputStream(file.toString());
	        BufferedReader bR = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
	        StringBuilder currentDoc = new StringBuilder();
	        Integer docIdMedline = 0;
	        String linea;
	        String lineaMod;
	        while ((linea = bR.readLine())!=null && linea.length() > 0) { //la longitud se mira para evitar lineas en blanco
	            lineaMod = linea.substring(0,2);
	            if(lineaMod.equals(".I")){
	                linea = linea.replace(".I ","");
	                docIdMedline = Integer.parseInt(linea);
	                linea = bR.readLine(); //el ".W"
	                linea = bR.readLine();
	                lineaMod = linea.substring(0,2);
	                currentDoc = new StringBuilder();
	                while (!lineaMod.equals(".I")){ //se itera para recorrer todas las líneas de la query
	                    if(currentDoc.length() == 0) { //se mira si es la primera vez que se hace append para incluír un espacio o no
	                        currentDoc.append(linea);
	                    }else {
	                        currentDoc.append(" "+linea);
	                    }
	                    linea = bR.readLine();
	                    if(linea != null && linea.length() > 0) {
	                        lineaMod = linea.substring(0, 2);
	                    }else{
	                        break;
	                    }
	                }
	                result.put(docIdMedline, currentDoc.toString());
	                if(linea != null && linea.length() > 0) {
	                    linea = linea.replace(".I ", "");
	                    docIdMedline = Integer.parseInt(linea);
	                }
	            }else if(lineaMod.equals(".W")){
	                linea = bR.readLine();
	                lineaMod = linea.substring(0,2);
	                currentDoc = new StringBuilder();
	                while (!lineaMod.equals(".I")){
	                    if(currentDoc.length() == 0) {
	                        currentDoc.append(linea);
	                    }else {
	                        currentDoc.append(" "+linea);
	                    }
	                    linea = bR.readLine();
	                    if(linea != null && linea.length() > 0) {
	                        lineaMod = linea.substring(0, 2);
	                    }else{
	                        break;
	                    }
	                }
	                result.put(docIdMedline, currentDoc.toString());
	                if(linea != null && linea.length() > 0) {
	                    linea = linea.replace(".I ", "");
	                    docIdMedline = Integer.parseInt(linea);
	                }
	            }
	        }
	        return result;
	    }

		
		//Lectura MED.REL
	    public static Map<Integer,List<Integer>> parseRel(Path file) throws FileNotFoundException, IOException{
	        Map<Integer, List<Integer>> result = new HashMap<>();
	        FileInputStream stream = new FileInputStream(file.toString());
	        BufferedReader bR = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
	        //StringBuilder currentDoc = new StringBuilder();
	        List<Integer> currentRelevant = new ArrayList<>();
	        Integer docId = 0;
	        String linea;
	        boolean newDoc = true;
	        /*Cada entrada tiene el estilo "1 0 13 1", dónde
	            1 es lineaNums[0]
	            0 es lineaNums[1]
	            13 es lineaNums[2]
	            1 es lineaNums[3]
	         */
	        while ((linea = bR.readLine())!=null && linea.length() > 0) {
	            String[] lineaNums = linea.split(" ");
	            if (Integer.parseInt(lineaNums[0]) != docId) {
	                if (docId != 0) {
	                    result.put(docId, currentRelevant);
	                }
	                newDoc = true;
	            }
	            if (newDoc) {
	                docId = Integer.parseInt(lineaNums[0]);
	                newDoc = false;
	                currentRelevant = new ArrayList<>();
	            }
	            if (!newDoc) {
	                currentRelevant.add(Integer.parseInt(lineaNums[2]));
	            }
	        }
	        if(linea == null){//Al final del archivo acaba con dos líneas en blanco. En este punto solo queda añadir a result lo de la última query
	            result.put(docId, currentRelevant);
	        }
	        return result;
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
	
	
	
	//Metodo para comprobar que el parametro de entrada es un numero positivo o 0
	private static int esNumeroValido(String argumento, String tipoArgumento) {
		
		int varSalida = 0;
		
		try { //Comprobamos si es un numero
			
			varSalida = Integer.parseInt(argumento);
			if (varSalida<=0) {
				System.err.println("ERROR: El numero de \""+ tipoArgumento +"\" especificado tiene que ser mayor que 0");
				System.exit(1);
			}//Si es un 0 o un numero negativo se devuelve un error
			
	    }
	    catch (NumberFormatException e){
	        System.out.println("ERROR: \""+ tipoArgumento +"\" no contiene un numero de entrada");
	        System.exit(1);;
	    }
		
		System.out.println("Numero de \""+ tipoArgumento +"\" especificado: "+ varSalida);
		return varSalida;
	}

	
	
	
	

}
