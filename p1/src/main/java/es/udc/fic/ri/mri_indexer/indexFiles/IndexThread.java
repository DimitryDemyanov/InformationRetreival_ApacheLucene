package es.udc.fic.ri.mri_indexer.indexFiles;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;



/*

import java.awt.List;

import java.io.FileInputStream;
import java.io.FileReader;

import java.io.StringReader;

import java.nio.file.FileVisitResult;

import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;

import java.util.ArrayList;

import java.util.Properties;
import java.util.StringTokenizer;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;

import org.apache.lucene.document.TextField;

import org.apache.lucene.index.IndexWriterConfig;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;*/







import es.udc.fic.ri.mri_indexer.LectorConfigProperties;



/**
 * 
 * Clase que tambien se reutilizan fragmentos de codigo empleados el año pasado
 *
 */
class ManagerIndexThread{
	

	private Path path = null;
	private IndexWriter writer = null;
	private int numberTopLines = 0;
	private int numberBottomLines = 0;
	
	public ManagerIndexThread(Path path, IndexWriter writer, int nTopLines, int nBottomLines) {
		this.path = path;
		this.writer = writer;
		this.numberTopLines = nTopLines;
		this.numberBottomLines = nBottomLines;
		
	}
	
	
	private static Boolean isAllowed(String file, String[] allowed_extensions) {

		return Arrays.stream(allowed_extensions)
				.map(x -> x.substring(1))
				.anyMatch(x -> file.endsWith(x));
		
	}
	
	//Para comprobar la extension del archivo
	public void start(boolean onlyFiles, ExecutorService exec) {
		
		try (Stream<Path> paths = Files.walk(path)){
			
			paths
			.filter(Files::isRegularFile)						
			.filter(x -> !onlyFiles || isAllowed(x.toString(), LectorConfigProperties.getParametro("onlyFiles").split(" ")))
			.forEach(path -> {
				final Runnable worker = new IndexThread(path, writer, numberTopLines, numberBottomLines);
				exec.execute(worker);
			});
				
		}catch (Exception e) {
			// TODO: handle exception
			 e.printStackTrace();
		}
				
	}
}// Fin de ManagerIndexThread




public class IndexThread implements Runnable {
	
	
	private final IndexWriter writer;
	private final Path file;
	private int numberTopLines = 0;
	private int numberBottomLines = 0;
	
	
	public IndexThread(final Path file, final IndexWriter writer, int nTopLines, int nBottomLines) {
		this.file = file;
		this.writer = writer;
		this.numberTopLines = nTopLines;
		this.numberBottomLines = nBottomLines;
		
	}
	
	//Leemos los atributos del fichero y ejecutamos
	public void run() {
		try {
			BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
			indexDoc(writer, file, attrs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private void indexDoc(IndexWriter writer, Path file, BasicFileAttributes atributos) throws IOException {

		try (FileInputStream stream = new FileInputStream(file.toString())){
			
			// Creamos un nuevo documento vacio
			Document doc = new Document();
			
			//Campo "path"
			doc.add(new StringField("path", file.toString(), Field.Store.YES));
			
			//Campo "hostname"
			doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
									
			//Campo "thread" actual
			doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
			
			//Campo "type"
			String tipo = "";
			if(atributos.isDirectory()){
				tipo = "directory";
			}
			else if(atributos.isSymbolicLink()){
				tipo = "symbolic link";
			}
			else if(atributos.isRegularFile()){
				tipo = "regular file";
			}
			else if(atributos.isOther()){
				tipo = "otro";
			}
			
			doc.add(new StringField("type", tipo, Field.Store.YES));

			
			//Campo "contents"
			doc.add(new TextField("contents",
					new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
			
			
			//Campo "contetsStored"
			FieldType type_stored = new FieldType();
			type_stored.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
			type_stored.setTokenized(true);
			type_stored.setStored(true);
			type_stored.setStoreTermVectors(true);
			type_stored.setStoreTermVectorPositions(true);
			type_stored.freeze();
	
			BufferedReader contenidoArchivo = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			int nLineasArchivo = 0; //Contador de lineas del archivo
			
			StringBuilder contenidoStored = new StringBuilder();
			String linea = null;
			
			while ((linea = contenidoArchivo.readLine())!=null) {
				contenidoStored.append(linea);
				contenidoStored.append(" ");
				nLineasArchivo++;		
			}
			
			doc.add(new Field("contentsStored", contenidoStored, type_stored));
			stream.getChannel().position(0);

			
			
			//Campo "OnlyTopLines"
			int i;
			if (numberTopLines!=-1) {
				
				BufferedReader topLinesContent = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
				StringBuilder contenidoToplinesStored = new StringBuilder();

				for (i=0; i<numberTopLines && i<nLineasArchivo; i++) {
					contenidoToplinesStored.append(topLinesContent.readLine());
					contenidoToplinesStored.append(" ");
				}
				doc.add(new Field("onlyTopLines", contenidoToplinesStored, type_stored));
				stream.getChannel().position(0);
			}
			
			
			//Campo "OnlyBottomLines"
			if (numberBottomLines!=-1) {	
				
				BufferedReader BottomLinesContent = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
				StringBuilder contenidoBottomlinesStored = new StringBuilder();
				
				for (i=0; i<nLineasArchivo-numberBottomLines; i++) {
					BottomLinesContent.readLine();
				}
				for (; i<nLineasArchivo; i++) {
					contenidoBottomlinesStored.append(BottomLinesContent.readLine());
					contenidoBottomlinesStored.append(" ");
				}
				doc.add(new Field("onlyBottomLines", contenidoBottomlinesStored, type_stored));
				stream.getChannel().position(0);
			}
			

			// Anadimos el tamano en Kb
			Long sizeFile = file.toFile().length();
			doc.add(new StringField("sizeKb", sizeFile.toString() , Field.Store.YES));
			
			
			//Leemos los atributos de la entrada con el contenido de las fechas de creeacion, modificacion y acceso
			FileTime creationTime = atributos.creationTime();
			FileTime lastAccessTime = atributos.lastAccessTime();
			FileTime lastModifiedTime = atributos.lastModifiedTime();
		
			
			//Campos convertidos a formato String
			doc.add(new StringField("creationTime", creationTime.toString(), Field.Store.YES));
			doc.add(new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));
			doc.add(new StringField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));
			

			//Campos convertidos a formatos reconocidos por Lucene
			
			//creationTimeLucene
			Date fechaCreationTime = new Date(creationTime.toMillis());
			String stringCreationTime = DateTools.dateToString(fechaCreationTime,Resolution.MILLISECOND);
			doc.add(new StringField("creationTimeLucene", stringCreationTime, Field.Store.YES));
			
			//lastAccessTimeLucene
			Date fechaLastAccessTime = new Date(creationTime.toMillis());
			String stringLastAccessTime = DateTools.dateToString(fechaLastAccessTime,Resolution.MILLISECOND);
			doc.add(new StringField("lastAccessTimeLucene", stringLastAccessTime, Field.Store.YES));
			
			//lastModifiedTimeLucene
			Date fechaLastModifiedTime = new Date(creationTime.toMillis());
			String stringLastModifiedTime = DateTools.dateToString(fechaLastModifiedTime,Resolution.MILLISECOND);
			doc.add(new StringField("lastAccessTimeLucene", stringLastModifiedTime, Field.Store.YES));
		
			
			if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
				// -openmode CREATE
				System.out.println("Thread "+Thread.currentThread().getName()+" adding "+ file);
				writer.addDocument(doc);
			}else {
				// El indice existe
				System.out.println("updating "+ file);
				writer.updateDocument(new Term("path", file.toString()), doc);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}