package com.wtp;

public class Singleton {
    private static final Singleton inst= new Singleton();

    private Singleton() {
        super();
    }

    public synchronized void writeToFile(Table table, String outputFile) {
        table.writeFile(outputFile);
    }

    public static Singleton getInstance() {
        return inst;
    }

}