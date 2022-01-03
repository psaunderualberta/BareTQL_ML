package com.wtp;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import java.text.NumberFormat;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class App 
{
    public static final String outputFile = "./Data/tables.csv";
    public static int tablesY = 0, tablesN = 0;
    public static int linesToRead = 2 * (int) Math.pow(10, 9);
    public static long timeStart, timeEnd;
    public static void main( String[] args )
    {    
        BufferedReader bzipFile;
        String line = "";
        File file = new File("D:/Over9000/Documents/Dev/WTP/Data Generation/enwiki-20200401-pages-articles.xml.bz2");
        String bzipFilename = file.toString();
        System.out.println(bzipFilename);
        Pattern listPattern = Pattern.compile("^\\s+<title>(List of [a-zA-Z0-9\\s]*?)</title>$");
        Matcher match;

        File dataFile = new File(outputFile); 
        dataFile.delete();

        ArrayList<String> titles = new ArrayList<>();

        try {
            bzipFile = getBufferedReaderForCompressedFile(bzipFilename);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Error occurred in main");
            return;
        }

        timeStart = System.currentTimeMillis();
        try {
            line = bzipFile.readLine();
            for (int i = 0; line != null && i < linesToRead; i++) {
                if (i % 100000 == 0) {
                    System.out.println(NumberFormat.getNumberInstance(Locale.US).format(i));
                }

                match = listPattern.matcher(line);

                if (match.find()) {
                    titles.add(match.group(1));
                    if (titles.size() >= 256) {
                        System.out.println("Parsing tables");
                        parsePage(titles);
                        titles.clear();
                    }
                }

                line = bzipFile.readLine();
            }

            parsePage(titles);
        } catch (Exception e) {
            System.out.println("Error occurred on line ? while reading bzip file");
        }

        timeEnd = System.currentTimeMillis();
        System.out.println((tablesY + tablesN) + " tables found");
        System.out.println(tablesY + " tables successfully processed");
        System.out.println("Success rate: " + Math.round((double) tablesY / (double) (tablesN + tablesY) * 100) + "%");
        System.out.println("Total time elapsed: " + (double) (timeEnd - timeStart) / 1000 + "s");

        return;
    }

    // https://stackoverflow.com/questions/4834721/java-read-bz2-file-and-uncompress-parse-on-the-fly
    public static BufferedReader getBufferedReaderForCompressedFile(String fileIn) throws FileNotFoundException, CompressorException {
        FileInputStream fin = new FileInputStream(fileIn);
        BufferedInputStream bis = new BufferedInputStream(fin);
        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
        return br2;
    }

    public static void parsePage(ArrayList<String> titles) {
        int coreCnt = Runtime.getRuntime().availableProcessors();
        ArrayList<Future<ArrayList<Integer>>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(coreCnt);
        for (String title : titles) {
            final Future<ArrayList<Integer>> f = executor.submit(new TableRunner(title, outputFile));
            futures.add(f);
        }

        for (Future<ArrayList<Integer>> future : futures) {
            try {
                final ArrayList<Integer> arr = future.get();
                tablesY += arr.get(0);
                tablesN += arr.get(1);
            } catch (InterruptedException | ExecutionException ex) {
            }
        }

        executor.shutdown();  
        try {
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            } 
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
