package cis5550.utils;

import java.util.*;

public class CollectionsUtils {
    public static <T> List<Collection<T>> partition(Collection<T> aCollection, int aPartitionSize) {
        List<Collection<T>> myPartitions = new LinkedList<>();

        Collection<T> myCurrentPartition = new HashSet<>();
        for (T myItem : aCollection) {
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
