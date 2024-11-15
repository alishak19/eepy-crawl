package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.List;

public class FlameIntersection {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        List<String> list1 = Arrays.asList(args);
        List<String> list2 = Arrays.asList("a", "b", "c", "c", "a");

        FlameRDD rdd1 = ctx.parallelize(list1);
        FlameRDD rdd2 = ctx.parallelize(list2);

        FlameRDD rdd = rdd1.intersection(rdd2);

        List<String> out = rdd.collect();

        String result = "";
        for (String s : out) {
            result = result + (result.equals("") ? "" : ",") + s;
        }

        ctx.output(result);
    }
}
