package es.udc.fic.ri.mri_searcher.trainingTestMedline;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.lucene.search.similarities.ClassicSimilarity;

public class TrainingTestMedline {

    public static Map<Integer,String> parseQueries(Path file) throws IOException{
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

    public static Map<Integer,List<Integer>> parseRel(Path file) throws IOException{
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

    public static float valorRecall(List<Integer> ranking, List<Integer> relevantes) {
        float result = 0;
        int numRelevantes = 0;
        for(int i = 0; i < ranking.size();i++){
            for(int j = 0; j < relevantes.size();j++){
                if(ranking.get(i).equals(relevantes.get(j))){
                    numRelevantes++;
                }
            }
        }

        if(relevantes.size() > 0) {
            result = (float) numRelevantes / (relevantes.size());
        }else{
            result = 0;
        }
        return result;
    }

    public static float valorPrecision(List<Integer> ranking, List<Integer> relevantes) {
        float result = 0;
        int numRelevantes = 0;
        for(int i = 0; i < ranking.size();i++){
            for(int j = 0; j < relevantes.size();j++){
                if(ranking.get(i).equals(relevantes.get(j))){
                    numRelevantes++;
                }
            }
        }

        if(ranking.size() > 0) {
            result = (float) numRelevantes / (ranking.size());
        }
        else result = 0;
        return result;
    }

    public static float valorMap(List<Integer> ranking, List<Integer> relevantes) {
        float result = 0;
        float sumaPrecisiones = 0;
        float prec = 0;
        //calcular precisión para cade relevante del ranking, sumarlas y luego dividirlo entre los relevantes totales
        for(int i = 0; i < ranking.size();i++){
            for(int j = 0; j < relevantes.size();j++){
                if(ranking.get(i).equals(relevantes.get(j))){
                    List<Integer> rankingParcial = new ArrayList<>();
                    for(int z = 0; z <= i;z++){
                        rankingParcial.add(ranking.get(z));
                    }
                    prec = valorPrecision(rankingParcial,relevantes);
                    sumaPrecisiones = sumaPrecisiones + prec;
                }
            }
        }

        if(relevantes.size() > 0) {
            result = (float) sumaPrecisiones / relevantes.size();
        }
        else{
            result = 0;
        }
        return result;
    }

    public static void main(String[] args) throws IOException {

        String usage = "TrainingTestMedline:\n"
                + "[{-evaljm int1-int2 int3-int4 | -evaltfidff int3-int4}]\n" //int1-int2 queries entrenamiento; int3-int4:queries test
                + "[-cut n]\n" //corte en el ranking
                + "[-metrica {P | R | MAP}]\n" //métrica computada
                + "[-indexin pathname]"; //ruta de la carpeta con el índice

        //Se mira si se pasan los parámetros necesarios y, si se pasa se coge su valor
        Boolean evaljm = null;
        Integer cut = null;
        String metrica = null;
        String indexPath = null;

        Integer[] limitesEntrenamiento = new Integer[2];
        Integer[] limitesTest = new Integer[2];

        Properties prop = new Properties();
        prop.load(new FileReader(".\\src\\main\\resources\\config.properties"));
        String queriesPath = null;
        queriesPath = prop.getProperty("queriesPath");
        String fileName = "medline.";


        for (int i = 0; i < args.length; i++) {
            if ("-evaljm".contentEquals(args[i])) {
                evaljm = true;
                String[] limites = args[i+1].split("-");
                limitesEntrenamiento[0] = Integer.valueOf(limites[0]);
                limitesEntrenamiento[1] = Integer.valueOf(limites[1]);
                limites = args[i+2].split("-");
                limitesTest[0] = Integer.valueOf(limites[0]);
                limitesTest[1] = Integer.valueOf(limites[1]);
                i+=2;
                fileName = fileName + "jm.";
            } else if ("-evaltfidf".contentEquals(args[i])) {
                evaljm = false;
                String[] limites = args[i+1].split("-");
                limitesTest[0] = Integer.parseInt(limites[0]);
                limitesTest[1] = Integer.parseInt(limites[1]);
                i++;
                fileName = fileName + "tfidf.";
            } else if ("-cut".contentEquals(args[i])) {
                cut = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-metrica".contentEquals(args[i])) {
                metrica = args[i + 1];
                i++;
            } else if ("-indexin".contentEquals(args[i])) {
                indexPath = args[i + 1];
                i++;
            }
        }

        if (evaljm == null) {
            System.out.println("Error: Eval is mandatory");
            System.out.println(usage);
            System.exit(1);
        }
        if (cut == null) {
            System.out.println("Error: Cut is mandatory");
            System.out.println(usage);
            System.exit(1);
        } else if (metrica == null || metrica.length() == 0) {
            System.out.println("Error: Metrica is mandatory");
            System.out.println(usage);
            System.exit(1);
        } else if (indexPath == null || indexPath.length() == 0) {
            System.out.println("Error: Indexin is mandatory");
            System.out.println(usage);
            System.exit(1);
        }

        IndexReader iReader = null;
        Directory directoiro = null;
        IndexSearcher iSearcher = null;
        QueryParser parser;
        Query query = null;
        float mejorPromedio = -1.0f;
        float mejorParametro = -1;

        try {
            directoiro = FSDirectory.open(Paths.get(indexPath));
            iReader = DirectoryReader.open(directoiro);
        } catch (CorruptIndexException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }

        iSearcher = new IndexSearcher(iReader);
        parser = new QueryParser("Contents", new StandardAnalyzer());

        Map<Integer,String> queriesMap = parseQueries(Paths.get(queriesPath+"MED.QRY"));
        Map<Integer, List<Integer>> queriesRel = parseRel(Paths.get(queriesPath+"MED.REL"));

        //En este bucle for se eliminan los parentesis de las querys ya que dan error al parsear las querys más adelante
        for(Integer r=1; r <= queriesMap.size();r++){
            String queryText = queriesMap.get(r);
            queryText = queryText.replaceAll("[(]", "");
            queryText = queryText.replaceAll("[)]", "");
            queriesMap.put(r,queryText);
        }

        Map<Integer,String> queriesEntrenamiento = new HashMap<>();
        Map<Integer,List<Integer>> queriesRelEntrenamiento = new HashMap<>();

        Map<Integer,String> queriesTest = new HashMap<>();
        Map<Integer,List<Integer>> queriesRelTest = new HashMap<>();

        int inicioRangoTest = -1, finRangoTest = -1;

        inicioRangoTest = limitesTest[0];
        finRangoTest = limitesTest[1];
        for (int i=inicioRangoTest; i<=finRangoTest; i++) { //Se cogen las queries para test
            queriesTest.put(i, queriesMap.get(i));
            queriesRelTest.put(i, queriesRel.get(i));
        }

        if (evaljm) {
            int inicioRangoEnt = -1, finRangoEnt = -1;
            inicioRangoEnt = limitesEntrenamiento[0];
            finRangoEnt = limitesEntrenamiento[1];
            for (int i = inicioRangoEnt; i <= finRangoEnt; i++) { //se cogen las queries para entrenamiento
                queriesEntrenamiento.put(i, queriesMap.get(i));
                queriesRelEntrenamiento.put(i, queriesRel.get(i));
            }

            fileName = fileName +"training."+Integer.toString(inicioRangoEnt)+"-"+Integer.toString(finRangoEnt)+
                    ".test."+Integer.toString(inicioRangoTest)+"-"+Integer.toString(finRangoTest)+"."
                    +metrica.toLowerCase()+Integer.toString(cut)+".";

            Map<Float,Map<Integer,Float>> salida = new HashMap<>();
            Map<Float,Float> promedios = new HashMap<>();
            Integer numQuerys = queriesEntrenamiento.size();
            for (float lz=0.1f; lz<=1.0f; lz+=0.1) {
                iSearcher.setSimilarity(new LMJelinekMercerSimilarity(lz));
                float valorMetrica = -1.0f;
                Map<Integer,Float> salidaQuery = new HashMap<>();
                float promedio = 0;
                for (Integer i : queriesEntrenamiento.keySet()) {
                    List<Integer> ranking = new ArrayList<>();
                    try {
                        query = parser.parse(queriesEntrenamiento.get(i).toLowerCase());
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    boolean reverse = false;
                    //topDocs es el ranking para la query i
                    TopDocs topDocs = iSearcher.search(query, cut, new Sort(new SortField("Contents", SortField.Type.SCORE, reverse)), true);

                    for (ScoreDoc s : topDocs.scoreDocs) {
                        ranking.add(s.doc);
                    }

                    if (metrica.equals("MAP")) {
                        valorMetrica = valorMap(ranking, queriesRelEntrenamiento.get(i));
                    } else if (metrica.equals("P")) {
                        valorMetrica = valorPrecision(ranking, queriesRelEntrenamiento.get(i));
                    } else if (metrica.equals("R")) {
                        valorMetrica = valorRecall(ranking, queriesRelEntrenamiento.get(i));
                    }

                    promedio = promedio + valorMetrica;
                    salidaQuery.put(i,valorMetrica);
                    //System.out.println("Query: "+i+". Lamda: "+lz+". Valor métrica: "+valorMetrica);
                }

                salida.put(lz,salidaQuery);

                promedio = promedio/numQuerys;
                promedios.put(lz,promedio);

                if (promedio>mejorPromedio) {
                    mejorParametro = lz;
                    mejorPromedio = promedio;
                }
            }

            //System.out.println("Mejor valor lambda: "+mejorParametro);
            iSearcher.setSimilarity(new LMJelinekMercerSimilarity(mejorParametro)); //Para la parte de test

            try{
                PrintWriter pw = new PrintWriter(new File(fileName+"training.csv"));
                StringBuilder sb = new StringBuilder();

                //Primera fila = cabecera
                sb.append("Corte="+cut.toString()+" & metrica="+metrica);
                sb.append(";");

                List<Map.Entry<Float, Map<Integer,Float>>> listLambda = new ArrayList<>(salida.entrySet());
                listLambda.sort(Map.Entry.comparingByKey());
                for(Map.Entry<Float, Map<Integer,Float>> entr : listLambda){ //una entrada en la cabecera para cada valor de lambda
                    sb.append("lambda "+entr.getKey()+";");
                }
                sb.append("\n");

                //una fila por cada query
                for(Integer cont=1; cont <= queriesEntrenamiento.size();cont++) {
                    sb.append("query"+Integer.toString(cont)+";");
                    for (Map.Entry<Float, Map<Integer, Float>> entr : listLambda) {
                        sb.append(entr.getValue().get(cont)+";");
                    }
                    sb.append("\n");
                }

                sb.append("Promedios:;"); //se añade una última fila con los promedios

                List<Map.Entry<Float, Float>> listPromedios = new ArrayList<>(promedios.entrySet());
                listLambda.sort(Map.Entry.comparingByKey());
                for(Map.Entry<Float, Float> prom : listPromedios){ //una entrada en la cabecera para cada valor de lambda
                    sb.append(prom.getValue()+";");
                }

                sb.append("\n");

                pw.write(sb.toString());
                pw.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }else{
            Similarity similaridad = new ClassicSimilarity();
            iSearcher.setSimilarity(similaridad); //Para la parte de test
            fileName = fileName +"training.null"+
                    ".test."+Integer.toString(inicioRangoTest)+"-"+Integer.toString(finRangoTest)+"."
                    +metrica.toLowerCase()+Integer.toString(cut)+".";
        }

        //Parte común: test
        float valorMetrica = -1.0f;
        float promedio = 0;
        Map<Integer,Float> salidaQuery = new HashMap<>();
        for (Integer i : queriesTest.keySet()) {
            List<Integer> ranking = new ArrayList<>();
            try {
                query = parser.parse(queriesTest.get(i).toLowerCase());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            boolean reverse = false;
            TopDocs topDocs = iSearcher.search(query, cut,new Sort(new SortField("Contents", SortField.Type.SCORE, reverse)), true);

            for (ScoreDoc s : topDocs.scoreDocs) {
                ranking.add(s.doc);
            }

            if (metrica.equals("MAP")) {
                valorMetrica = valorMap(ranking, queriesRelTest.get(i));
            } else if (metrica.equals("P")) {
                valorMetrica = valorPrecision(ranking, queriesRelTest.get(i));
            } else if (metrica.equals("R")) {
                valorMetrica = valorRecall(ranking, queriesRelTest.get(i));
            }

            promedio = promedio + valorMetrica;
            salidaQuery.put(i,valorMetrica);
        }

        promedio = promedio/(queriesTest.size());

        /*if (metrica.equals("MAP")) {
            System.out.println("Promedio de map: "+promedio);
        } else if (metrica.equals("P")) {
            System.out.println("Promedio de precisión: "+promedio);
        } else if (metrica.equals("R")) {
            System.out.println("Promedio de recall: "+promedio);
        }*/

        try {
            PrintWriter pw = new PrintWriter(new File(fileName + "test.csv"));
            StringBuilder sb = new StringBuilder();

            //Primera fila = cabecera
            sb.append("Corte=" + cut.toString() + " & metrica=" + metrica);
            sb.append(";");
            if(evaljm) {
                sb.append("lambda " + mejorParametro + ";");
            }else {
                sb.append("lambda of  ClassicSimilarity;");
            }
            sb.append("\n");

            //una fila por cada query
            List<Map.Entry<Integer, Float>> listaResultadosTest = new ArrayList<>(salidaQuery.entrySet());
            listaResultadosTest.sort(Map.Entry.comparingByKey());
            for (Map.Entry<Integer, Float> resultadoQueryTest : listaResultadosTest) {
                sb.append("query " + resultadoQueryTest.getKey() + ";");
                sb.append(resultadoQueryTest.getValue() + ";");
                sb.append("\n");
            }

            sb.append("Promedio:;"); //se añade una última fila con el promedio
            sb.append(promedio);
            sb.append("\n");

            pw.write(sb.toString());
            pw.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        //System.out.println("\n");

        //Se imprimen los dos csv (o uno solo) por pantalla
        BufferedReader br = null;
        if(evaljm){
            try {
                br =new BufferedReader(new FileReader(fileName+"training.csv"));
                String line = br.readLine();
                while (null!=line) {
                    String [] fields = line.split(";");
                    System.out.println(Arrays.toString(fields));
                    line = br.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null!=br) {
                    br.close();
                }
            }
        }

        System.out.println(""); //para separar las tablas

        try {
            br =new BufferedReader(new FileReader(fileName+"test.csv"));
            String line = br.readLine();
            while (null!=line) {
                String [] fields = line.split(";");
                System.out.println(Arrays.toString(fields));
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null!=br) {
                br.close();
            }
        }
    }
}
