/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.conf;

/**
 *
 * @author kaustubh
 */
public class Configuration {
    
        public static String schema = "saga1";//annodb
        public static String username = "root";//test
        public static String password = "kaustubh167";//test
        public static String driver= "com.mysql.jdbc.Driver";//test
        
        
        public static String logFile = "/usr/local/Cellar/apache-spark/1.6.1/sparkTestLog.txt";
        public static String url = "jdbc:mysql://localhost:3306/" + schema + "?user=" + username + "&password=" + password;

        public static String master = "spark://igm-spark1:7077";
        public static String csvFilePath= "~/genotypes.csv";
        //public static String cvFile="/Users/kaustubh/Desktop/all_samples_called_variant.txt";
        public static String cvFile="/nfs/external/igm-interns/PopSeQL/data/newdb/parquet/called_variant/";
        public static String rcFile="/nfs/external/igm-interns/PopSeQL/data/newdb/parquet/read_coverage/";
        //public static String rcFile="/Users/kaustubh/Desktop/all_samples_read_coverage_1024.txt";
        public static String sample="/nfs/external/igm-interns/PopSeQL/spark/samples.txt";
}
