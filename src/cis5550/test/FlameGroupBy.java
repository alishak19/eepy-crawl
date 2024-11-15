package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.List;

public class FlameGroupBy {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        List<String> list1 = Arrays.asList(args);

        FlameRDD rdd1 = ctx.parallelize(list1);

        FlamePairRDD rdd = rdd1.groupBy(s -> s);

        List<FlamePair> out = rdd.collect();

        String result = "";
        for (FlamePair fp : out) {
            result = result + (result.equals("") ? "" : ",") + fp.toString();
        }

        ctx.output(result);
    }
}
