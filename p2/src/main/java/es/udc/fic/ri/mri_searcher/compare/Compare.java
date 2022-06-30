package es.udc.fic.ri.mri_searcher.compare;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

public class Compare {
	
    //Comprobamos que el nombre de los resultados se corresponda
    public static boolean namesCheck(String archivo1, String archivo2) {
        boolean result = false;
        int punto = 0;
        int totalChar = 0;

        int i = 0;
        while (i < archivo1.length() && punto < 5) {
            char c = archivo1.charAt(i);
            totalChar += 1;        
            if (c == '.') {
                punto += 1;
            }
            i++;
        }
        archivo1 = archivo1.substring(totalChar, archivo1.length());

        //Reseteamos las variables
        i = 0;
        punto = 0;
        totalChar = 0;

        while (i < archivo2.length() && punto < 5) {
            char c = archivo2.charAt(i);
            totalChar += 1;        
            if (c == '.') {
                punto += 1;
            }
            i++;
        }
        archivo2 = archivo2.substring(totalChar, archivo2.length());

        result = archivo1.equals(archivo2);

        return result;
    }

    //Metodo para saber el numero de elementos de result
    public static int getNum(String archivo) {
        int result = 0;
            
        int punto = 0;
        int guion = 0;
        int charFirstNum = 0;
        int charSecondNum = 0;
        int charSlash = 0;

        int i = 0;
        while (i < archivo.length() && punto < 5) {
            char c = archivo.charAt(i);
            charFirstNum += 1;   
            charSecondNum += 1; 
            charSlash += 1;     
            if (c == '.') {
                punto += 1;
            }
            i++;
        }

        while (i < archivo.length() && guion < 1) {
            char c = archivo.charAt(i);
            charSecondNum += 1; 
            charSlash += 1;     
            if (c == '-') {
                guion += 1;
            }
            i++;
        }

        while (i < archivo.length() && punto < 6) {
            char c = archivo.charAt(i);
            charSecondNum += 1;
            if (c == '.') {
                punto += 1;
            }
            i++;
        }
        
        int num1 = Integer.parseInt(archivo.substring(charFirstNum, charSlash-1));
        int num2 = Integer.parseInt(archivo.substring(charSlash, charSecondNum-1));

        result = num2 - num1 + 1;

        return result;
    }

    //Metodo para almacenar en un array los valores de un .csv
    public static double[] toArray(Path path, Integer num) throws NumberFormatException, IOException{

		double[] result = new double[num];
		FileInputStream stream = new FileInputStream(path.toString());
		LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		int i = 0;
		String line;

		while ((line = reader.readLine()) != null) {
            //Ignoramos primera y ultima linea
            if (reader.getLineNumber() != 1 && reader.getLineNumber() != num) {
                String[] lines = line.split(";");
                Double value = Double.parseDouble(lines[1]);
                result[i] = value;
                i++;
            }
		}

		return result;
		
	}

	public static void main (String[] args) throws NumberFormatException, IOException {

        String usage = "Compare: \n" 
				+ "[-results RESULTS1.csv RESULTS2.csv]\n"
				+ "[-test t|wilcoxon alpha]\n";	
	
        //Variables necesarias
        String results1 = null, results2 = null;
        Path results1P = null, results2P = null;
        double[] results1A = null, results2A = null;

        boolean test = false, wilcoxon = false;
        Double alpha = null;
	
        //Lectura de argumentos
        for (int i = 0;i<args.length;i++) {
			if ("-results".contentEquals(args[i])) {
				results1 = args[i+1];
				results2 = args[i+2];
				i += 2;
			} else if ("-test".contentEquals(args[i])) {
                if ("t".contentEquals(args[i+1])) {
                    test = true;
				    alpha = Double.parseDouble(args[i+2]);
				    i++;
                } else if ("wilcoxon".contentEquals(args[i+1])) {
                    wilcoxon = true;
                    i++;
                }
			} else if ((args.length < 6) || ("-help".contentEquals(args[i]))) {
				System.err.println("Error: Faltan argumentos");
				System.out.println(usage);
				System.exit(1);
			}
		}

        //Seguimos si los nombres coinciden
        if (namesCheck(results1, results2)) {

            System.out.println("Nombres de los archivos: OK\nCalculando...\n");

            results1P = Paths.get(results1);
            results2P = Paths.get(results2);

            results1A = toArray(results1P, getNum(results1));
            results2A = toArray(results2P, getNum(results2));

            if (test) {
                TTest testT = new TTest();
                System.out.println("Test de significancia estadistica (t-test)");
                boolean result = testT.tTest(results1A, results2A, alpha);
                double pValor = testT.tTest(results1A, results2A);
                System.out.println("Resultado: " + result + ". P-Valor: " + pValor);
    
            } else if (wilcoxon) {
                System.out.println("Test de significancia estadistica (Wilcoxon)");
                double pValor = new WilcoxonSignedRankTest().wilcoxonSignedRankTest(results1A, results2A, true);
                System.out.println("P-Valor: " + pValor);
            }

        } else {
            System.out.println("Error: no coinciden los nombres de los archivos");
        }
    }
}
