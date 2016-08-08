/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.utils;

import com.atav.genotypes.beans.NonCarrier;
import com.atav.genotypes.beans.Variant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 *
 * @author kaustubh
 */
public class Utils {
        public static short getCovValue(char letter) {
        switch (letter) {
            case 'a':
            default:
                return -1;
            case 'b':
                return 3;
            case 'c':
                return 10;
            case 'd':
                return 20;
            case 'e':
                return 201;
        }
    }
    
    public static Function< Tuple2<String, Tuple2<
                                        Map<String, Variant>, Map<String, TreeMap<Integer, String>>>>, Variant> joinMapper = (Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>> t1) -> {
                                            String block_id=t1._1;
                                            //List<Variant> vars=new ArrayList<>(((t1._2)._1).values());
                                            
                                            Variant v=new ArrayList<>(((t1._2)._1).values()).get(0);
                                            
                                            Set<String> nonCarrierSamps;
                                            nonCarrierSamps=SampleManager.sampleSet;
                                            nonCarrierSamps.removeAll(v.getCarrierMap().keySet());
                                            for (String samp: nonCarrierSamps){
                                                TreeMap<Integer,String> covMap=((t1._2)._2).get(samp);
                                                Map.Entry<Integer, String> key=covMap.ceilingEntry(0==v.getPos()%1024?1024:v.getPos()%1024);
                                                short cv=getCovValue(key.getValue().charAt(0));
                                                v.getNonCarrierMap().put(samp, new NonCarrier(samp,cv));
                                            }
                                            return v;
        };

}
