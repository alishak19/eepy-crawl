package cis5550.utils;

import java.util.Set;

public class Stopwords {
    private static final Set<String> STOPWORDS = Set.of(
            "a", "an", "the", "and", "or", "but", "is", "are", "was", "were",
            "be", "has", "he", "it", "its", "that", "will", "i", "me", "my",
            "myself", "we", "our", "ours", "ourselves", "you", "your",
            "yours", "yourself", "yourselves", "him", "his", "himself",
            "she", "her", "hers", "herself", "they", "them",
            "their", "theirs", "themselves", "what", "which", "who", "whom", "this",
            "these", "those", "am", "been", "being", "have", "had", "having",
            "do", "does", "did", "doing", "about", "against", "between", "into", "through",
            "during", "before", "after", "above", "below", "to", "from", "up", "down", "in",
            "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
            "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
            "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
            "so", "than", "too", "very", "s", "t", "can", "just", "don", "should", "now"
    );

    public static final boolean isStopWord(String word) {
        return STOPWORDS.contains(word);
    }
}
