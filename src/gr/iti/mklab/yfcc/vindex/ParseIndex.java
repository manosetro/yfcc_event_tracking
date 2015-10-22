package gr.iti.mklab.yfcc.vindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;

import gr.iti.mklab.visual.datastructures.Linear;

public class ParseIndex {
	
	public static final int vectorLength = 1024;
	public static final int maxNumVectors = 100000000;
	
	public static final boolean readOnly = false;
	public static final boolean loadIndexInMemory = false;
	
	public static final boolean countSizeOnLoad = true;
	public static final int loadCounter = 0;
	
	public static void main(String[] args) throws Exception {
		
		String BDBEnvHome = "/second_disk/vIndex/yfcc100m_linear";
		
		Linear index = new Linear(vectorLength, maxNumVectors, readOnly, BDBEnvHome,
				loadIndexInMemory, countSizeOnLoad, loadCounter);

		
		File directory = new File("/second_disk/yfcc100m_features");
		String[] extensions = {"gz"};
		
		Iterator<File> it = FileUtils.iterateFiles(directory, extensions, true);
		while(it.hasNext()) {
			File file = it.next();
			System.out.println("Index: " + file);
			index(file, index);
		}
	}
	
	public static void index(File file, Linear index) throws Exception {
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(
				new FileInputStream(file)), "UTF-8"));
		
		String line = "";
		while ((line = reader.readLine()) != null) {
			String[] lineParts = line.split("\t");
			String url = lineParts[0];
			
			String encodedId = IndexUtils.encodeId(url);
			
			String vectorString = lineParts[1];
			vectorString = vectorString.replace("[", "");
			vectorString = vectorString.replace("]", "");
			String[] elementsString = vectorString.split(", ");
			
			if (elementsString.length != vectorLength) {
				continue;
			}
			
			try {
			    double[] vector = new double[vectorLength];
			    for (int i = 0; i < vectorLength; i++) {
				    vector[i] = Double.parseDouble(elementsString[i]);
			    }

			    index.indexVector(encodedId, vector);
			}
			catch(Exception e) {
				continue;
			}
		}
		reader.close();
	}

}
