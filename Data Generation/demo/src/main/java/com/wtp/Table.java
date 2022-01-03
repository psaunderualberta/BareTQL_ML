package com.wtp;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringJoiner;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Table {
    private String title;
    private ArrayList<ArrayList<String>> table;
    private Element tableElement;

    public Table(String title, Element table) {
        this.title = title;
        this.table = new ArrayList<ArrayList<String>>();
        this.tableElement = table;
        
        return;
    }

    public boolean parse() {
        Elements cells = tableElement.select("td, th");
        Elements rows = tableElement.select("tr");
        String text;
        int rowI = 0;
        int colI = 0;
        int rowspan;
        
        if (!basicChecks(rows, cells)) {
            return false;
        }

        for (int i = 0; i < rows.size(); i++) {
            table.add(new ArrayList<String>());
            for (int j = 0; j < rows.get(0).select("td,th").size(); j++) {
                table.get(i).add(null);
            }
        }

        try {
            for (Element row : rows) {
                colI = 0;
                for (Element cell : row.select("td,th")) {
                    text = cell.text();
                    while (table.get(rowI).get(colI) != null) {
                        colI++;
                    }
    
                    table.get(rowI).set(colI, text);
    
                    if (cell.attributes().hasKey("rowspan")) {
                        rowspan = Integer.parseInt(cell.attr("rowspan"));
                        for (int rowSpanCount = 0; rowSpanCount < rowspan; rowSpanCount++) {
                            table.get(rowI + rowSpanCount).set(colI, text);
                        }
                    }
                }
                rowI++;
            }   
        } catch (Exception e) {
            return false;
        }

        if (!postChecks()) {
            return false;
        }
        
        return true;
    }

    private boolean basicChecks(Elements rows, Elements cells) {

        // No tiny tables
        if (rows.size() < 5) {
            return false;
        }

        // No nested tables
        if (rows.select("table").size() != 0) {
            return false;
        }

        // No images
        if (rows.select("img").size() != 0) {
            return false;
        }

        // No bulleted lists
        if (rows.select("li,ul").size() != 0) {
            return false;
        }

        // No column spans, not conducive to the type of table
        // we are interested in.
        for (Element cell : cells) {
            if (cell.attributes().hasKey("colspan")) {
                return false;
            }
        }
        
        return true;
    }

    public boolean postChecks() {

        // Don't want large paragraphs of text in the result set
        for (ArrayList<String> row : table) {
            for (String cell : row) {
                if (cell != null && cell.length() > 200) {
                    return false;
                }
            }
        }
        return true;
    }

    public int writeFile(String filename) {
        try {
            FileWriter writer = new FileWriter(filename, true);
            writer.write(title + "\n");
            String delimiter = ",";
            StringJoiner joiner = new StringJoiner(delimiter);
    
            for (ArrayList<String> row : table) {
                for (String cell : row) {
                    if (cell != null) {
                        cell = cell.replaceAll("\"", "\\\"");
                    }
                    
                    joiner.add("\"" + cell + "\"");
                }
                
                writer.write(joiner.toString() + "\n");
                joiner = new StringJoiner(delimiter);
            }

            writer.close();
        } catch (IOException e) {
            e.getStackTrace();
        }

        return 0;
    }
}
