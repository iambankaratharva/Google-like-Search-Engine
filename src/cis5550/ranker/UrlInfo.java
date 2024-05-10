package cis5550.ranker;

import java.util.Comparator;

public class UrlInfo implements Comparator<UrlInfo> {

    private final String url;
    private double tfidf;
    private double pagerank = 1;
    private double score;
    private boolean isCalculated;

    public UrlInfo() {
        this.url = "";
    }

    public UrlInfo(String url, double tfidf, double pagerank) {
        this.url = url;
        this.tfidf = tfidf;
        this.pagerank = pagerank;
        this.isCalculated = false;
    }

    public double calculateScore() {
        if (isCalculated) {
            return score;
        }
        score = tfidf*pagerank;
        isCalculated = true;
        return score;
    }

   public void addTfidf(double additionalTfidf) {
       tfidf += additionalTfidf;
       this.isCalculated = false;  // Reset since TF-IDF has changed
   }

    @Override
    public String toString() {
        return url + " " + score;
    }

    public double getScore() {
        return score;
    }

    public double getTfIdf() {
        return tfidf;
    }


    public String getUrl() {
        return url;
    }

    @Override
    public int compare(UrlInfo o1, UrlInfo o2) {
        // TODO Auto-generated method stub
        return (o1.getScore() < o2.getScore() ? 1 : -1);
    }
}
