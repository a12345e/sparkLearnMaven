package infra.elastic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * The few Elasticsearch admin calls this project needs, over plain HTTP.
 *
 * <p>Index creation, deletion and mapping are given as raw JSON on purpose.
 * Elasticsearch already has a mapping language; wrapping it in a Java DSL
 * would mean maintaining a translation for no gain, and settings you have not
 * thought of yet would be unreachable. The Spark connector does the row
 * traffic - this only does structure.
 *
 * <p>{@code HttpURLConnection} rather than a client library because the
 * elasticsearch-hadoop connector already shades its own HTTP stack, and adding
 * a second one to the classpath buys nothing for six calls.
 */
public final class ElasticRest {

    private final String baseUrl;

    public ElasticRest(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public String baseUrl() {
        return baseUrl;
    }

    public boolean indexExists(String index) {
        return send("HEAD", "/" + index, null).status == 200;
    }

    public boolean aliasExists(String alias) {
        return send("HEAD", "/_alias/" + alias, null).status == 200;
    }

    /** Deletes the index; an index that is not there is not an error. */
    public void deleteIndex(String index) {
        Response response = send("DELETE", "/" + index, null);
        if (response.status != 200 && response.status != 404) {
            throw new IllegalStateException("could not delete index " + index + ": " + response);
        }
    }

    /**
     * Creates the index.
     *
     * @param body the full creation body: {@code {"settings":{...},"mappings":{...}}}
     */
    public void createIndex(String index, String body) {
        Response response = send("PUT", "/" + index, body);
        if (response.status != 200) {
            throw new IllegalStateException("could not create index " + index + ": " + response);
        }
    }

    /** Adds fields to an existing mapping. Elasticsearch allows adding, never retyping. */
    public void addFields(String index, String propertiesJson) {
        Response response = send("PUT", "/" + index + "/_mapping", propertiesJson);
        if (response.status != 200) {
            throw new IllegalStateException("could not add fields to " + index + ": " + response);
        }
    }

    /** Makes recent writes visible to search. */
    public void refresh(String index) {
        send("POST", "/" + index + "/_refresh", null);
    }

    public long count(String index) {
        Response response = send("GET", "/" + index + "/_count", null);
        // The body is {"count":4,"_shards":{...}} and the first "count" in it
        // is the one wanted. Read it by hand rather than adding a JSON library
        // for a single number.
        int at = response.body.indexOf("count");
        int colon = at < 0 ? -1 : response.body.indexOf(':', at);
        if (colon < 0) {
            throw new IllegalStateException("no count in response for " + index + ": " + response);
        }
        int end = colon + 1;
        while (end < response.body.length() && Character.isDigit(response.body.charAt(end))) {
            end++;
        }
        return Long.parseLong(response.body.substring(colon + 1, end).trim());
    }

    public String mapping(String index) {
        return send("GET", "/" + index + "/_mapping", null).body;
    }

    public String search(String index, int size) {
        return send("GET", "/" + index + "/_search?size=" + size, null).body;
    }

    /** Turns automatic index creation on or off for the whole cluster. */
    public void autoCreateIndex(boolean enabled) {
        Response response = send("PUT", "/_cluster/settings",
                "{\"persistent\":{\"action.auto_create_index\":\"" + enabled + "\"}}");
        if (response.status != 200) {
            throw new IllegalStateException("could not set action.auto_create_index: " + response);
        }
    }

    private Response send(String method, String path, String body) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(baseUrl + path).openConnection();
            connection.setRequestMethod(method);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(30000);
            if (body != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                OutputStream out = connection.getOutputStream();
                try {
                    out.write(body.getBytes(StandardCharsets.UTF_8));
                } finally {
                    out.close();
                }
            }
            int status = connection.getResponseCode();
            return new Response(status, read(status < 400
                    ? connection.getInputStream() : connection.getErrorStream()));
        } catch (IOException e) {
            throw new UncheckedIOException(method + " " + baseUrl + path + " failed", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static String read(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = in.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        in.close();
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    /** One HTTP answer. */
    private static final class Response {

        private final int status;
        private final String body;

        Response(int status, String body) {
            this.status = status;
            this.body = body;
        }

        @Override
        public String toString() {
            return status + " " + body;
        }
    }
}
