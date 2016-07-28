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
        public static String username = "spark";//test
        public static String password = "spark167";//test
        public static String driver= "com.mysql.jdbc.Driver";//test
        
        
        public static String logFile = "/usr/local/Cellar/apache-spark/1.6.1/sparkTestLog.txt";
        public static String url = "jdbc:mysql://localhost:3306/" + schema + "?user=" + username + "&password=" + password;
        public static String master = "spark://igm-it-spare.local:7077";
}
