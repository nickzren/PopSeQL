package utils;

import com.esotericsoftware.kryo.Kryo;
import function.genotype.base.*;
import function.genotype.collapsing.*;
import function.genotype.vargeno.VarGenoOutput;
import global.*;
import java.util.HashMap;
import org.apache.spark.serializer.KryoRegistrator;

/**
 *
 * @author nick
 */
public class Registrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        // genotype base class
        kryo.register(CalledVariant.class);
        kryo.register(Carrier.class);
        kryo.register(Gene.class);
        kryo.register(NonCarrier.class);
        kryo.register(Output.class);
        kryo.register(Sample.class);

        // collapsing class
        kryo.register(CollapsingSummary.class);
        kryo.register(CollapsingGeneSummary.class);
        kryo.register(CollapsingOutput.class);

        // list var geno class
        kryo.register(VarGenoOutput.class);

        // global class
        kryo.register(Data.class);
        kryo.register(Index.class);
        
        kryo.register(HashMap.class);
    }

}
