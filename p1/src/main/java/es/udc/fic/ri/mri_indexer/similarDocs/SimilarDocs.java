package es.udc.fic.ri.mri_indexer.similarDocs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class SimilarDocs {

	private static Set<String> terms = new HashSet<>();
	private static Map<String, Double> calculated_df = new HashMap<>();
	
	 public static void main(String[] args) throws IOException {

		 String usage = "SimilarDocs:\n"
				 + "[-index INDEX_PATH]\n" //ruta carpeta indice
				 + "[-doc DOC_ID]\n" //nº documento
				 + "[-field CAMPO]\n" //campo con TermVectors
				 + "[-top NUMBER_N]\n" //un entero que sirve para limitar el nº de documentos similares a mostrar
				 + "[-rep {bin | tf | tfxidf}]\n"; //se indica una representación

		 //Se mira si se pasan los parámetros necesarios y, si se pasa se coge su valor
		 String index = null;
		 String doc = null;
		 String field = null;
		 String top = null;
		 String rep = null;

		 for (int i = 0; i < args.length; i++) {

			 if ("-index".contentEquals(args[i])) {
				 index = args[i + 1];
				 i++;
			 } else if ("-doc".contentEquals(args[i])) {
				 doc = args[i + 1];
				 i++;
			 } else if ("-field".contentEquals(args[i])) {
				 field = args[i + 1];
				 i++;
			 } else if ("-top".contentEquals(args[i])) {
				 top = args[i + 1];
				 i++;
			 } else if ("-rep".contentEquals(args[i])) {
				 rep = args[i + 1];
				 i++;
			 } else if ((args.length != 1)) {
				 System.out.println("Error: ");
				 System.out.println(usage);
				 System.exit(1);
			 }
		 }

		 if (index == null || index.length() == 0) {
			 System.out.println("Error: Index is mandatory");
			 System.exit(1);
		 }
		 if (doc == null || doc.length() == 0) {
			 System.out.println("Error: Doc is mandatory");
			 System.exit(1);
		 } else if (field == null || field.length() == 0) {
			 System.out.println("Error: Field is mandatory");
			 System.exit(1);
		 } else if (top == null || top.length() == 0) {
			 System.out.println("Error: Top is mandatory");
			 System.exit(1);
		 } else if (rep == null || rep.length() == 0) {
			 System.out.println("Error: Representation is mandatory");
			 System.exit(1);
		 }

		 //Se convierte el tipo de string a int del campo doc, y se almacena en docID
		 int docId = Integer.parseInt(doc);

		 //Inicializamos IndexReader y comprobamos que la apertura de la carpeta index sea satisfactoria
		 Directory directory = null;
		 DirectoryReader indexReader = null;
		 try {
			 directory = FSDirectory.open(Paths.get(index)); //se abre el índice
			 indexReader = DirectoryReader.open(directory);
		 } catch (CorruptIndexException e1) {
			 System.out.println("Graceful message: exception " + e1);
			 e1.printStackTrace();
		 } catch (IOException e1) {
			 System.out.println("Graceful message: exception " + e1);
			 e1.printStackTrace();
		 }


		 try {
			 List<Map<String, Double>> lista = new ArrayList<>();
			 Map<Integer, Double> docsSimilaritys = new HashMap<>();

			 //1 y 2- Se obtinenen los term vectors de los documentos y se representan según el campo "-rep"
			 //mainDoc contiene pares (term,value)
			 Map<String, Double> mainDoc = getTermFrequencies(indexReader, docId, field, rep);
			 for (int i=0; i < indexReader.maxDoc(); i++) { //Se meten en una lista
				 lista.add(getTermFrequencies(indexReader, i, field, rep));
			 }

			 int i = 0;
			 RealVector mainDocV = toRealVector(mainDoc);
			 for (Map<String, Double> par : lista) {
				 RealVector listV = toRealVector(par);
				 //Se compara cada documento con el documento principal para calcular la similitud
				 Double similarity = getCosineSimilarity(mainDocV, listV);
				 //Se almacena la similud
				 docsSimilaritys.put(i++, similarity);
			 }
			 
			 //3- Se obtienen los n más similares al indicado en "-doc" según la representación "-rep"
			 	//Para ello se miran los de mayor valor en la lista "docsSimilaritys"
			 DirectoryReader r2 = indexReader;
			 docsSimilaritys.entrySet().stream()
					 .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) //reverseOrder para coger de más a menos similitud
					 .limit(Long.parseLong(top)) //se establece el límite según el valor del argumento pasado "top"
					 .forEach(x -> {
								 try {
									 Document document = r2.document(x.getKey());
									 //4- Se viisualiza el resultado, indicando el docID Lucene y la ruta del documento target y de los similares
									 System.out.println("Doc with ID "+x.getKey()+" in '"+document.get("path")+ "' with similarity "+x.getValue());
									 //System.out.println("Doc with ID "+x.getKey()+" in '"+document.get("path"));
								 } catch (IOException e) {
									 e.printStackTrace();
								 }
							 }
					 );
			 r2.close();
			 indexReader.close();

		 } catch (IOException e) {
			 e.printStackTrace();
		 }

	 }

	private static double getCosineSimilarity(RealVector v1, RealVector v2) {
		return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
	}

	private static Map<String, Double> getTermFrequencies(IndexReader reader, int docId, String field, String rep) throws IOException {

		Terms vector = reader.getTermVector(docId, field); //Se coge el termVector del documento con docId

		TermsEnum termsEnum = null;
		termsEnum = vector.iterator();
		Map<String, Double> frecuencies = new HashMap<>();
		Double value = 0d;
		BytesRef text = null;
		while ((text = termsEnum.next()) != null) {
			String term = text.utf8ToString();
			double freq = termsEnum.totalTermFreq();
			terms.add(term); //Se añade a "terms" cada term del Term Vector obtenido antes

			//Se mira la forma de representación, y según una u otra se le da un valor a "value"
			if ("bin".contentEquals(rep)) {
				value = 1d;
			}else if("tf".contentEquals(rep)) {
				value = freq;
			}else if("tf-idf".contentEquals(rep)) {
				double df = 0;
				if (calculated_df.containsKey(term)) {
					df = calculated_df.get(term);
				}else {
					df = reader.docFreq(new Term(field,text));
					calculated_df.put(term,df);
				}
				double idf = (reader.numDocs() / df);
				Double tf_idf = (freq * idf);
				value = tf_idf;
			}

			frecuencies.put(term, value); //Se mete en frecuencies el par (term,value)
		}
		return frecuencies; //Se devuelve un hashMap con los pares
	}

	private static RealVector toRealVector(Map<String, Double> map) {
		RealVector vector = new ArrayRealVector(terms.size());
		int i = 0;
		for (String term : terms) {
			double value = map.containsKey(term) ? map.get(term) : 0;
			vector.setEntry(i++, value);
		}
		return (RealVector) vector.mapDivide(vector.getL1Norm());
		// la división por la norma L1 del vector no es necesaria
		// pero tampoco afecta al calculo del coseno
	}
}