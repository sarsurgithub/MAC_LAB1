package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;


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
        var result = ctx.query("""
                SELECT imdb.id as imdb_id, tomatoes.viewer.rating as tomatoes_rating,
                imdb.rating as imdb_rating
                FROM `mflix-sample`._default.movies
                WHERE tomatoes.viewer.rating > 0 AND ABS(tomatoes.viewer.rating - imdb.rating) > 7;
                """
        );
        return result.rowsAs(JsonObject.class);
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
        var result = ctx.query("""
                SELECT c.email, COUNT(c) as cnt
                FROM `mflix-sample`._default.comments c
                GROUP BY c.email
                ORDER BY COUNT(c) DESC
                LIMIT 10;
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<String> greatReviewers() {
        var result = ctx.query("""
                        SELECT RAW c.email
                        FROM `mflix-sample`._default.comments c
                        GROUP BY c.email
                        HAVING COUNT(*) > 300
                        """
        );
        return result.rowsAs(String.class);
    }


    public List<JsonObject> bestMoviesOfActor(String actor) {
        var query = """
            SELECT imdb.id as imdb_id, imdb.rating, m.`cast`
            FROM `mflix-sample`._default.movies m
            WHERE imdb.rating > 8 AND ISNUMBER(imdb.rating) AND ? IN `cast`
            """;
        var result = ctx.query(query, QueryOptions.queryOptions().parameters(JsonArray.from(actor)));
        return result.rowsAs(JsonObject.class);
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
        var result = ctx.query("""
                SELECT m._id AS movie_id, m.title
                FROM `mflix-sample`._default.movies m
                WHERE ARRAY_LENGTH(m.directors) > 20;
                """
        );
        return result.rowsAs(JsonObject.class);
    }


    public List<JsonObject> commentsOfDirector1(String director) {
        // need to create index
        var result = ctx.query("SELECT m._id AS movie_id, comments.text\n" +
                               "FROM `mflix-sample`._default.movies m\n" +
                               "JOIN `mflix-sample`._default.comments ON comments.movie_id = m._id\n" +
                               "WHERE \"" + director + "\" IN m.directors\n"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        var result = ctx.query("SELECT c.movie_id, c.text\n" +
                               "FROM `mflix-sample`._default.comments c\n" +
                               "WHERE c.movie_id IN (SELECT RAW m._id FROM `mflix-sample`._default.movies m WHERE \"" + director + "\" IN m.directors)\n"
        );
        return result.rowsAs(JsonObject.class);
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        var result = ctx.query("UPDATE `mflix-sample`._default.theaters AS t\n" +
                "SET t.schedule = ARRAY_REMOVE(t.schedule, s) FOR s IN t.schedule WHEN s.movieId = \"" + movieId + "\" AND s.hourBegin < '18:00:00' END\n" +
                "WHERE ANY s IN t.schedule SATISFIES s.movieId = \"" + movieId +"\"AND s.hourBegin < '18:00:00' END;"
        );
        return Integer.parseInt(result.toString());
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
