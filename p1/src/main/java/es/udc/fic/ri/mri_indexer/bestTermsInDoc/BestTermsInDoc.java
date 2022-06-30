package es.udc.fic.ri.mri_indexer.bestTermsInDoc;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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

public class BestTermsInDoc {

	//Obtener TF, DF o IDF a partir de un Index
	public static Map<String, Double> getFrequencies(IndexReader reader, int docID, String fieldName, String type) throws IOException {
		
		Map<String, Double> output = new HashMap<>();
		
		Terms terms = reader.getTermVector(docID, fieldName);
		final TermsEnum termsEnum = terms.iterator();
		
		while (termsEnum.next() != null) {
			String termString = termsEnum.term().utf8ToString();
			Term term = new Term(fieldName, termString);

			//Se realiza la operación correspondiente dependiendo de lo que se indique
			switch (type) {
				case "tf":
					int freqTerm = (int) reader.totalTermFreq(term);
					output.put(termString, (double) freqTerm);
					break;

				case "df":
					int freqDoc = reader.docFreq(term);
					output.put(termString, (double) freqDoc);
					break;

				case "idf":
					double freqIdf = (1.0d + Math.log10((1.0d + reader.getDocCount(fieldName)) / (1.0d + reader.docFreq(new Term(fieldName, termString)))));
					output.put(termString, freqIdf);
					break;

				case "tfxidf":
					double tf = (double) termsEnum.totalTermFreq();
					double freqTFxIDF = tf * (1.0d + Math.log10((1.0d + reader.getDocCount(fieldName)) / (1.0d + reader.docFreq(new Term(fieldName, termString)))));
					output.put(termString, freqTFxIDF);
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
				case "tf":
					Double freqTerm = (double) reader.totalTermFreq(term);

					if (output.containsKey(freqTerm)) {
						List <String> list = output.get(freqTerm);
						list.add(termString);
						output.replace(freqTerm, list);

					} else {
						List <String> list = new ArrayList<>();
						list.add(termString);
						output.put(freqTerm, list);

					}
					break;

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

				case "tfxidf":
					double tf = (double) termsEnum.totalTermFreq();
					double freqTFxIDF = tf * (1.0d + Math.log10((1.0d + reader.getDocCount(fieldName)) / (1.0d + reader.docFreq(new Term(fieldName, termString)))));
		
					if (output.containsKey(freqTFxIDF)) {
						List <String> list = output.get(freqTFxIDF);
						list.add(termString);
						output.replace(freqTFxIDF, list);
					} else {
						List <String> list = new ArrayList<>();
						list.add(termString);
						output.put(freqTFxIDF, list);
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

		String usage = "WriteIndex:\n" 
				+ "[-index INDEX_PATH]\n"
				+ "[-docID DOCUMENTO]\n"
				+ "[-field CAMPO]\n"
				+ "[-top NUMBER_N]\n"
				+ "[-order {tf | df | idf | tfxidf}]\n"
				+ "[-outputfile FICHERO SALIDA]\n\n";	

		//System.out.println(usage);

    	String index = null;
		String docIDString = null;
		String field = null;
		String topString = null;
		String order = "tf";
		String outputFile = null;

		Integer docIDInt = null;
		Integer topInt = null;

		//Lectura de argumentos
		for (int i = 0; i < args.length; i++) {
			
			
			
			if (args[i].equals("-index")) {
				index = args[i + 1];
				i++;

			} else if (args[i].equals("-docID")) {
				docIDString = args[i + 1];
				i++;

			} else if (args[i].equals("-field")) {
				field = args[i + 1];
				i++;

			} else if (args[i].equals("-top")) {
				topString = args[i + 1];
				i++;

			} else if (args[i].equals("-order")) {
				order = args[i + 1];
				i++;

			} else if (args[i].equals("-outputfile")) {
				outputFile = args[i + 1];
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
		} else if (docIDString == null || docIDString.length() == 0) {
			System.out.println("Error: Term is mandatory");
			System.exit(1);
		} else if (topString == null || topString.length() == 0) {
            System.out.println("Error: Top is mandatory");
            System.exit(1);
		}
		
    	
		//Parser de String a Integer de las variables necesarias
		if (topString != null) {
			topInt = Integer.parseInt(topString);
		}
		
		if (docIDString != null) {
			docIDInt = Integer.parseInt(docIDString);
		}
		

		//Apertura del índice Index
    	try {
			directory = FSDirectory.open(Paths.get(index));
			indexReader = DirectoryReader.open(directory);

		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	
    	//Si se desea, el resultado se volcará en un fichero en el mismo directorio
    	FileWriter output = null;
		PrintWriter writer = null;
		
    	if (outputFile != null) {
    		try {
				output = new FileWriter(outputFile);
				writer = new PrintWriter(output);

    		} catch (Exception e) {
				e.printStackTrace();
    		}
    	}

		//Posiciones
		int pos = 1;

		//Depende del order, realizamos la operación
		if (order.equals("tf")) {
			System.out.println("Ordenado por TF\n");

			Map <Double, List<String>> tf = getFrequenciesList(indexReader, docIDInt, field, "tf");
			Map <String, Double> df = getFrequencies(indexReader, docIDInt, field, "df");
			Map <String, Double> idf = getFrequencies(indexReader, docIDInt, field, "idf");
			Map <String, Double> tfxidf = getFrequencies(indexReader, docIDInt, field, "tfxidf");
			List <Double> list = new ArrayList<>(tf.keySet());

			//Ordenamos la lista con los resultados obtenidos
			Collections.sort(list);
			Collections.reverse(list);

			for (Double i:list) {
				for (String j:tf.get(i)) {
					if (pos <= topInt) {
						if (outputFile != null) {
							writer.println(pos + ". Term: " + j + "   tf: " + i + 
							"   df: " + df.get(j) + "   idf: " + idf.get(j) + "   tf x idf: " + tfxidf.get(j));
						} else {
							System.out.println(pos + ". Term: " + j + "   tf: " + i + 
							"   df: " + df.get(j) + "   idf: " + idf.get(j) + "   tf x idf: " + tfxidf.get(j));
						}
					}

					pos++;
				}
			}
			

		} else if (order.equals("df")) {
			System.out.println("Ordenado por DF\n");

			Map<String, Double> tf = getFrequencies(indexReader, docIDInt, field, "tf");
			Map<Double, List<String>> df = getFrequenciesList(indexReader, docIDInt, field, "df");
			Map <String, Double> idf = getFrequencies(indexReader, docIDInt, field, "idf");
			Map <String, Double> tfxidf = getFrequencies(indexReader, docIDInt, field, "tfxidf");
			List <Double> list = new ArrayList<>(df.keySet());

			//Ordenamos la lista con los resultados obtenidos
			Collections.sort(list);
			Collections.reverse(list);

			for (Double i:list) {
				for (String j:df.get(i)) {
					if (pos <= topInt) {
						if (outputFile != null) {
							writer.println(pos + ". Term: " + j + "   tf: " + tf.get(j) + 
							"   df: " + i + "   idf: " + idf.get(j) + "   tf x idf: " + tfxidf.get(j));
						} else {
							System.out.println(pos + ". Term: " + j + "   tf: " + tf.get(j) + 
							"   df: " + i + "   idf: " + idf.get(j) + "   tf x idf: " + tfxidf.get(j));
						}
					}

					pos++;
				}
			}

		} else if (order.equals("idf")) {
			System.out.println("Ordenado por IDF\n");

			Map<String, Double> tf = getFrequencies(indexReader, docIDInt, field, "tf");
			Map<String, Double> df = getFrequencies(indexReader, docIDInt, field, "df");
			Map<Double, List<String>> idf = getFrequenciesList(indexReader, docIDInt, field, "idf");
			Map <String, Double> tfxidf = getFrequencies(indexReader, docIDInt, field, "tfxidf");
			List <Double> list = new ArrayList<>(idf.keySet());

			//Ordenamos la lista con los resultados obtenidos
			Collections.sort(list);
			Collections.reverse(list);

			for (Double i:list) {
				for (String j:idf.get(i)) {
					if (pos <= topInt) {
						if (outputFile != null) {
							writer.println(pos + ". Term: " + j + "   tf: " + tf.get(j) + 
							"   df: " + df.get(j) + "   idf: " + i + "   tf x idf: " + tfxidf.get(j));
						} else {
							System.out.println(pos + ". Term: " + j + "   tf: " + tf.get(j) + 
							"   df: " + df.get(j) + "   idf: " + i + "   tf x idf: " + tfxidf.get(j));
						}
					}

					pos++;
				}
			}

		} else if (order.equals("tfxidf")) {
			System.out.println("Ordenado por TFxIDF\n");

			Map<String, Double> tf = getFrequencies(indexReader, docIDInt, field, "tf");
			Map<String, Double> df = getFrequencies(indexReader, docIDInt, field, "df");
			Map<String, Double> idf = getFrequencies(indexReader, docIDInt, field, "idf");
			Map<Double, List<String>> tfxidf = getFrequenciesList(indexReader, docIDInt, field, "tfxidf");
			List <Double> list = new ArrayList<>(tfxidf.keySet());

			//Ordenamos la lista con los resultados obtenidos
			Collections.sort(list);
			Collections.reverse(list);

			for (Double i:list) {
				for (String j:tfxidf.get(i)) {
					if (pos <= topInt) {
						if (outputFile != null) {
							writer.println(pos + ". Term: " + j + "   tf: " + tf.get(j) + 
							"   df: " + df.get(j) + "   idf: " + idf.get(j) + "   tf x idf: " + i);
						} else {
							System.out.println(pos + ". Term: " + j + "   tf: " + tf.get(j) + 
							"   df: " + df.get(j) + "   idf: " + idf.get(j) + "   tf x idf: " + i);
						}
					}

					pos++;
				}
			}
		}
		
		//Cerramos el fichero con el resultado
		if (outputFile!=null) {
			writer.close();
			output.close();

		}
    	
	}

}
