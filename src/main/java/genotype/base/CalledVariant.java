/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package genotype.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.spark.sql.Row;

/**
 *
 * @author ferocha
 */
public class CalledVariant {
    
    private int variantId;
    private HashMap<Integer, Carrier> carrierMap = new HashMap<>();
    private HashMap<Integer, NonCarrier> noncarrierMap = new HashMap<>();
    
    public CalledVariant(int vid, Iterator<Row> carrierDataIt,Iterator<Row> nonCarrierDataIt) {
        variantId = vid;
        while(carrierDataIt.hasNext()) {
            Row r = carrierDataIt.next();
            carrierMap.put(r.getInt(r.fieldIndex("sample_id")), new Carrier(r));
        }
        
        while(nonCarrierDataIt.hasNext()) {
            Row r = nonCarrierDataIt.next();
            noncarrierMap.put(r.getInt(r.fieldIndex("sample_id")), new NonCarrier(r));
        }
    }
    
    public Iterator<String> getStringRowIterator() {
        ArrayList<String> l = new ArrayList<>(carrierMap.size()+noncarrierMap.size());
        for(Carrier c : carrierMap.values())
            l.add(c.simpleString(variantId));
        for(NonCarrier nc : noncarrierMap.values())
            l.add(nc.simpleString(variantId));
        return l.iterator();
    }
    
}
