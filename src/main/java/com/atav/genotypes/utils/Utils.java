/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atav.genotypes.utils;

import com.atav.genotypes.VarGenoOutput;
import com.atav.genotypes.beans.NonCarrier;
import com.atav.genotypes.beans.Variant;
import function.variant.base.Output;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author kaustubh
 */
public class Utils implements Serializable {
    private static final long serialVersionUID = 80L;
    public Broadcast<Map<String,Integer>> broadCastPheno;
    public Broadcast<Set<String>> broadCastSamples;
    public Utils(Broadcast<Map<String,Integer>> in, Broadcast<Set<String>> in2){
        this.broadCastPheno=in;
        this.broadCastSamples=in2;
    }
    
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
        
    public Function< Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>>, ArrayList<Variant>> joinMapper = 
            new Function<Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>>, ArrayList<Variant>>() {
                
            @Override
            public ArrayList<Variant> call(Tuple2<String, Tuple2<Map<String, Variant>, Map<String, TreeMap<Integer, String>>>> t1) throws Exception {
                ArrayList<Variant> vars =new ArrayList<>(((t1._2)._1).values());
                ((t1._2)._1).clear();
                //if (null!=v.getPos()){
                for (Variant v: vars){
                    Set<String> nonCarrierSamps=new HashSet<>();
                    nonCarrierSamps.addAll(broadCastSamples.value());
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
                    }
                ((t1._2)._2).clear();
                return vars;
            }
        };
    
    
    
    
     public Function<ArrayList<Variant>,List<String>> outputMapper= new Function<ArrayList<Variant>, List<String>>() {
        @Override
        public List<String> call(ArrayList<Variant> vars) {
            ArrayList<String> listStrings= new ArrayList<>();
            for(Variant t1: vars){
            Output o= new Output();
            t1.getCarrierMap().entrySet().stream().forEach((c) -> {
                o.addSampleGeno(c.getValue().getGenotype(),
                        broadCastPheno.value().get(
                                c.getKey()
                        ));
            });
            t1.getNonCarrierMap().entrySet().stream().forEach((nc) -> {
                o.addSampleGeno(nc.getValue().getGenotype(), 
                        broadCastPheno.value().get(
                                nc.getKey()
                        ));
            });
            t1.setPheno(broadCastPheno.value().get(t1.getSampleID())==0? "case":"ctrl");
            listStrings.addAll((new VarGenoOutput(t1,o)).getStringList());
            }
            return listStrings;
        }
    };
           
     public FlatMapFunction<List<String>,String> elementsOfList = new FlatMapFunction<List<String>, String>() {
        @Override
        public Iterator<String> call(List<String> t){
            return t.iterator();
        }
    };
             
             
             
             //(Variant t1) -> {
         //return (new VarGenoOutput(t1)).toString();
//    };

}
