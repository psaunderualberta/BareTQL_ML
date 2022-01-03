package com.wtp;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class TableRunner implements Callable<ArrayList<Integer>> {
    private String title, outputFile;
    private int y, n;

    public TableRunner(String title, String outputFile) {
        this.title = title;
        this.outputFile = outputFile;
    }

    @Override
    public ArrayList<Integer> call() {
        try {
            Document doc = Jsoup.connect("https://en.wikipedia.org/wiki/" + title.replace(" ", "_")).get();

            for (Element tableElement : doc.select("table")) {
                if (tableElement.hasClass("wikitable")) {
                    Table table = new Table(title, tableElement);
                    if (table.parse()) {
                        Singleton.getInstance().writeToFile(table, outputFile);
                        y++;
                    } else {
                        n++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        ArrayList<Integer> result = new ArrayList<>();
        result.add(y);
        result.add(n);

        return result;
    }
    
}
