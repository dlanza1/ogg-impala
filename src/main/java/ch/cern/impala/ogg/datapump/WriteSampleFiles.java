package ch.cern.impala.ogg.datapump;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

import org.apache.commons.io.FileUtils;

public class WriteSampleFiles {
	
	private static File dataDirectory = new File("output");

	private static File controlFile = new File(dataDirectory.getAbsolutePath() + "/control-file.control");
	
	private static int ROWS_PER_SECOND = 60000;
	
	public static void main(String[] args) throws IOException {
		//Delete previous data
		FileUtils.deleteDirectory(dataDirectory);
		dataDirectory.mkdir();
		
		while(true){
			try {
				long startTime = System.currentTimeMillis();
				
				File file = writeDataFile();
				
				writeControlFile(file);

				System.out.println("Created file: " + file.getName() + " ("
						+ ROWS_PER_SECOND + " rows, "
						+ (System.currentTimeMillis() - startTime) + " ms)");
			} catch (FileException e) {
				e.printStackTrace();
			}
		}
	}

	private static File writeDataFile() throws FileException {
		long startTime = System.currentTimeMillis();
		Random r = new Random();
		
		File temporalFile = new File(dataDirectory.getAbsolutePath()
				+ "/data-file-" + getTimestamp() + ".tmp");
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(temporalFile));
        	
			for (int i = 0; i < ROWS_PER_SECOND; i++) {
				long variable_id = r.nextLong();
				String utc_stamp = "2015-07-12:04:32:12.123123123";
				double value = r.nextDouble();
				
				writer.write(variable_id + "," + utc_stamp + "," + value);
				writer.newLine();
			}
			
			while(System.currentTimeMillis() - startTime < 1000);
			
            try {
                writer.close();
            } catch (Exception e) {}
			
			//Rename file to final extension
			File finalFile = new File(dataDirectory.getAbsolutePath()
					+ "/" + temporalFile.getName()
					.substring(0, temporalFile.getName().indexOf(".")) + ".csv");
			temporalFile.renameTo(finalFile);
			
			//Delete temporal file
			temporalFile.delete();
			
			return finalFile;
		} catch (IOException e) {
			throw new FileException(temporalFile);
		} finally {
            try {
                writer.close();
            } catch (Exception e) {}
        }
	}
	
	private static void writeControlFile(File file) throws FileException {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(controlFile, true));
        	
			writer.write(file.getName() + ",");
		} catch (IOException e) {
			throw new FileException(controlFile);
		} finally {
            try {
                writer.close();
            } catch (Exception e) {}
        }
	}

	public static String getTimestamp() {
		Calendar cal = Calendar.getInstance();
		
		int month = (cal.get(Calendar.MONTH) + 1);
		int day = cal.get(Calendar.DAY_OF_MONTH);
		int hour = cal.get(Calendar.HOUR);
		int minute = cal.get(Calendar.MINUTE);
		int second = cal.get(Calendar.SECOND);

		return new String().concat(cal.get(Calendar.YEAR) + "-")
								.concat((month < 10 ? "0" + month : month) + "-")
								.concat((day < 10 ? "0" + day : day) + "-")
								.concat((hour < 10 ? "0" + hour : hour) + "-")
								.concat((minute < 10 ? "0" + minute : minute) + "-")
								.concat((second < 10 ? "0" + second : second) + "-")
								.concat(System.currentTimeMillis() + "");
	}
}
