package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;


public class Requests {
    private final Cluster ctx;

    public Requests(Cluster cluster) {
        this.ctx = cluster;
    }

    public List<String> getCollectionNames() {
        var result = ctx.query("""
                        SELECT RAW r.name
                        FROM system:keyspaces r
                        WHERE r.`bucket` = "mflix-sample";
                        """
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> hiddenGem() {
        var result = ctx.query("""
                SELECT m.title
                FROM `mflix-sample`._default.movies m
                WHERE m.tomatoes.critic.rating = 10 AND m.tomatoes.viewer IS MISSING
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<String> greatReviewers() {
        var result = ctx.query("""
                        SELECT DISTINCT RAW c.email
                        FROM `mflix-sample`._default.comments c
                        GROUP BY c.email
                        HAVING COUNT(*) > 300
                        """
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> plentifulDirectors() {
        var result = ctx.query("""
                SELECT dir AS director_name, COUNT(*) AS count_film
                FROM `mflix-sample`._default.movies m
                UNNEST m.directors dir
                GROUP BY dir
                HAVING COUNT(*)  > 30
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> confusingMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
