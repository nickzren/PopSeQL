/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.utils;

import com.atav.genotypes.beans.NonCarrier;
import com.atav.genotypes.beans.Variant;
import global.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 *
 * @author kaustubh
 */
public class Utils implements Serializable {
    private static final long serialVersionUID = 80L;
    
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
        
    public Function< Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>>, Variant> joinMapper = 
            new Function<Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>>, Variant>() {
                
            private final Set<String> samples;
                {
                    this.samples=new HashSet<>();
                    this.samples.addAll(SampleManager.broadCastSamples.value());
                }
                
            @Override
            public Variant call(Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>> t1) throws Exception {
                Variant v=new ArrayList<>(((t1._2)._1).values()).get(0);
                
                //if (null!=v.getPos()){
                    Set<String> nonCarrierSamps=new HashSet<>();
                    nonCarrierSamps.addAll(samples);
                    nonCarrierSamps.removeAll(v.getCarrierMap().keySet());
                    nonCarrierSamps
                            .stream().forEach((samp) -> {
                                
                                if (null!=((t1._2)._2).get(samp)){
                                v.getNonCarrierMap()
                                        .put(samp, new NonCarrier(samp,  getCovValue(((t1._2)._2)
                                                .get(samp)
                                                .ceilingEntry(0==v.getPos()%1024?1024:v.getPos()%1024)
                                                .getValue()
                                                .charAt(0))));
                                }else{
                                    short inv=-1;
                                    v.getNonCarrierMap()
                                        .put(samp,new NonCarrier(samp,inv));
                                }
                            });
                //}
                
                return v;
            }
        };

}
