package es.udc.fic.ri.mri_indexer.bestTermsInColl;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class BestTermsInColl {
	
	//Obtener TF, DF o IDF a partir de un Index
	public static Map<String, Double> getIDFFrequencies(IndexReader reader, int docID, String fieldName, String type) throws IOException {
		
		Map<String, Double> output = new HashMap<>();
		
		Terms terms = reader.getTermVector(docID, fieldName);
		final TermsEnum termsEnum = terms.iterator();
		
		while (termsEnum.next() != null) {
			String termString = termsEnum.term().utf8ToString();
			Term term = new Term(fieldName, termString);

			//Se realiza la operación correspondiente dependiendo de lo que se indique
			switch (type) {
				case "df":
					int freqDoc = reader.docFreq(term);
					output.put(termString, (double) freqDoc);
					break;

				case "idf":
					double freqIdf = (1.0d + Math.log10((1.0d + reader.getDocCount(fieldName)) / (1.0d + reader.docFreq(new Term(fieldName, termString)))));
					output.put(termString, freqIdf);
					break;
			}
		}

		return output;
	}

	//Obtener Lista de TF, DF o IDF a partir de un Index
	public static Map<Double, List<String>> getFrequenciesList(IndexReader reader, int docID, String fieldName, String type) throws IOException {
		
		Map<Double, List<String>> output = new HashMap<>();
		
		Terms terms = reader.getTermVector(docID, fieldName);
		final TermsEnum termsEnum = terms.iterator();
		
		while (termsEnum.next() != null) {
			String termString = termsEnum.term().utf8ToString();
			Term term = new Term(fieldName, termString);

			//Se realiza la operación correspondiente dependiendo de lo que se indique
			switch (type) {
				case "df":
					Double freqDoc = (double) reader.docFreq(term);

					if (output.containsKey(freqDoc)) {
						List <String> list = output.get(freqDoc);
						list.add(termString);
						output.replace(freqDoc, list);
		
					} else {
						List <String> list = new ArrayList<>();
						list.add(termString);
						output.put(freqDoc, list);
		
					}
					break;

				case "idf":
					double freqIdf = (1.0d + Math.log10((1.0d + reader.getDocCount(fieldName)) / (1.0d + reader.docFreq(new Term(fieldName, termString)))));
			
					if (output.containsKey(freqIdf)) {
						List <String> list = output.get(freqIdf);
						list.add(termString);
						output.replace(freqIdf, list);
					} else {
						List <String> list = new ArrayList<>();
						list.add(termString);
						output.put(freqIdf, list);
					}
					break;
			}
		}

		return output;
	}

	public static void main(String[] args) throws IOException {   
    	
		//Variables necesarias
		Directory directory = null;
		DirectoryReader indexReader = null;

    	String usage = "BestTermsInColl:\n" 
				+ "[-index INDEX_PATH]\n"
				+ "[-field CAMPO]\n"
				+ "[-top N_IDFLOG10]\n"
				+ "[-rev N_DF]\n";	

		//System.out.println(usage);
				
    	String index = null;
		String field = null;
		Boolean rev = false;
		String topString = null;
	        
		Integer topInt = null;

		//Lectura de argumentos
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-index")) {
				index = args[i + 1];
				i++;

			} else if (args[i].equals("-field")) {
				field = args[i + 1];
				i++;

			} else if (args[i].equals("-top")) {
				topString = args[i + 1];
				i++;

			} else if (args[i].equals("-rev")) {
				rev = true;
				i++;
			} else if ((args.length != 1)) {
				System.out.println("Error: ");
				System.out.println(usage);
				System.exit(1);
			}
		}

		//Comporbacion de argumentos
		if (index == null || index.length() == 0) {
			System.out.println("Error: Index is mandatory");
			System.exit(1);
		} else if (field == null || field.length() == 0) {
			System.out.println("Error: DocID is mandatory");
			System.exit(1);
		} else if (topString == null || topString.length() == 0) {
            System.out.println("Error: Top is mandatory");
            System.exit(1);
		}
    	
    	//Parser de String a Integer de las variables necesarias
		if (topString != null) {
			topInt = Integer.parseInt(topString);
		}
    	
    	//Apertura del índice Index
    	try {
			directory = FSDirectory.open(Paths.get(index));
			indexReader = DirectoryReader.open(directory);

		} catch (IOException e) {
			e.printStackTrace();
		}
    	
		//Posiciones
		int pos = 1;

    	if (rev != true) {
			System.out.println("Ordenado por IDF\n");

			Map<Double, List<String>> results = new HashMap<>();

			//Bucle que recorre todos los Doc existentes en el Index
			int numDoc = indexReader.maxDoc();

			for (int i = 0; i < numDoc; i++) {

				System.out.println("Procesando... DocID: " + i);

				Map<Double, List<String>> idf = getFrequenciesList(indexReader, i, field, "idf");
				List<Double> list = new ArrayList<>(idf.keySet());

				for (Double j:list) {
					for (String k:idf.get(j)) {
						
						if (results.containsKey(j)) {
							List <String> aux = results.get(j);
							aux.add(k);
							results.replace(j, aux);
	
						} else {
							List <String> aux = new ArrayList<>();
							aux.add(k);
							results.put(j, aux);
						}
					}
				}
			}

			//Volvemos a ordenar la lista
			List<Double> sortedKeys = new ArrayList<>(results.keySet());
			Collections.sort(sortedKeys);
			Collections.reverse(sortedKeys);

			System.out.println();

			//Mostramos el resultado
			for (Double i:sortedKeys) {
				for (String j:results.get(i)) {
					if (pos <= topInt) {
						System.out.println(pos + ". Term: " + j + "   idf: " + i);
					}
					pos++;
				}
			}

		} else {
			System.out.println("Ordenado por DF\n");

			Map<Double, List<String>> results = new HashMap<>();

			//Bucle que recorre todos los Doc existentes en el Index
			int numDoc = indexReader.maxDoc();

			for (int i = 0; i < numDoc; i++) {

				System.out.println("Procesando... DocID: " + i);

				Map<Double, List<String>> idf = getFrequenciesList(indexReader, i, field, "df");
				List<Double> list = new ArrayList<>(idf.keySet());

				for (Double j:list) {
					for (String k:idf.get(j)) {
						
						if (results.containsKey(j)) {
							List <String> aux = results.get(j);
							aux.add(k);
							results.replace(j, aux);
	
						} else {
							List <String> aux = new ArrayList<>();
							aux.add(k);
							results.put(j, aux);
						}
					}
				}
			}

			//Volvemos a ordenar la lista
			List<Double> sortedKeys = new ArrayList<>(results.keySet());
			Collections.sort(sortedKeys);
			Collections.reverse(sortedKeys);

			System.out.println();

			//Mostramos el resultado
			for (Double i:sortedKeys) {
				for (String j:results.get(i)) {
					if (pos <= topInt) {
						System.out.println(pos + ". Term: " + j + "   df: " + i);
					}
					pos++;
				}
			}
		}
	}
}
