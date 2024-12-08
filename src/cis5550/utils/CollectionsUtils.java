package cis5550.utils;

import java.util.*;

public class CollectionsUtils {
    public static <T> List<Set<T>> partition(Set<T> aSet, int aPartitionSize) {
        List<Set<T>> myPartitions = new LinkedList<>();

        Set<T> myCurrentPartition = new HashSet<>();
        for (T myItem : aSet) {
            myCurrentPartition.add(myItem);
            if (myCurrentPartition.size() == aPartitionSize) {
                myPartitions.add(myCurrentPartition);
                myCurrentPartition = new HashSet<>();
            }
        }

        if (!myCurrentPartition.isEmpty()) {
            myPartitions.add(myCurrentPartition);
        }

        return myPartitions;
    }
}
