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
    
        public static String schema = "annodb";//annodb
        public static String username = "test";//test
        public static String password = "test";//test
        public static String driver= "com.mysql.jdbc.Driver";//test
        
        
        public static String logFile = "/usr/local/Cellar/apache-spark/1.6.1/sparkTestLog.txt";
        public static String url = "jdbc:mysql://localhost:3306/" + schema + "?user=" + username + "&password=" + password;
        public static String csvFilePath= "/Users/zr2180/Desktop/genotypes.csv";
        
}
