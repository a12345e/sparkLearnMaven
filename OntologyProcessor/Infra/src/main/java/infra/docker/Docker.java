package infra.docker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Running {@code docker} and waiting for what it starts.
 *
 * <p>Shelling out rather than using a docker library, because the compose files
 * under {@code docker/} are the source of truth and this only needs to say
 * "up", "is it listening" and "which port did you publish". A library would be
 * a dependency for three commands.
 */
public final class Docker {

    private Docker() {
    }

    /** @return stdout of the command, with stderr folded in */
    public static String run(File dir, String... command) {
        Result result = attempt(dir, Collections.<String, String>emptyMap(), command);
        if (result.exitCode != 0) {
            throw new IllegalStateException("`" + String.join(" ", command) + "` failed ("
                    + result.exitCode + "):\n" + result.output);
        }
        return result.output;
    }

    /** As {@link #run}, with extra environment for the docker process (compose reads it for ${...}). */
    public static String run(File dir, Map<String, String> env, String... command) {
        Result result = attempt(dir, env, command);
        if (result.exitCode != 0) {
            throw new IllegalStateException("`" + String.join(" ", command) + "` failed ("
                    + result.exitCode + "):\n" + result.output);
        }
        return result.output;
    }

    /** Runs without throwing; inspect {@link Result#exitCode}. */
    public static Result attempt(File dir, Map<String, String> env, String... command) {
        List<String> full = new ArrayList<String>(Arrays.asList(command));
        ProcessBuilder builder = new ProcessBuilder(full).redirectErrorStream(true);
        if (dir != null) {
            builder.directory(dir);
        }
        builder.environment().putAll(env);

        Process process = null;
        try {
            process = builder.start();
            StringBuilder output = new StringBuilder();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), Charset.defaultCharset()));
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
            int exitCode = process.waitFor();
            return new Result(exitCode, output.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("could not run " + full, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running " + full, e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }

    /** @return whether something accepts a TCP connection there */
    public static boolean tcpOpen(String host, int port, int timeoutMs) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException notListening) {
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing useful to do.
            }
        }
    }

    /** @return whether a GET returns 2xx */
    public static boolean httpOk(String url, int timeoutMs) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setConnectTimeout(timeoutMs);
            connection.setReadTimeout(timeoutMs);
            connection.setRequestMethod("GET");
            int status = connection.getResponseCode();
            return status >= 200 && status < 300;
        } catch (IOException notServing) {
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Polls until {@code ready} is true.
     *
     * @throws IllegalStateException naming {@code what}, if it never becomes ready
     */
    public static void await(String what, long timeoutMs, Ready ready) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (ready.isReady()) {
                return;
            }
            sleep(1000);
        }
        throw new IllegalStateException(what + " was not ready within " + (timeoutMs / 1000) + "s");
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    /**
     * @return the OntologyProcessor directory, found by walking up from the
     *         working directory until the {@code docker/} tree appears
     */
    public static File projectRoot() {
        File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
        while (dir != null) {
            if (new File(dir, "docker/hadoop313hive313/docker-compose.yml").isFile()) {
                return dir;
            }
            dir = dir.getParentFile();
        }
        throw new IllegalStateException("could not find the project root (no docker/hadoop313hive313) "
                + "above " + System.getProperty("user.dir"));
    }

    /** Something to wait for. */
    public interface Ready {
        boolean isReady();
    }

    /** The outcome of a command. */
    public static final class Result {

        public final int exitCode;
        public final String output;

        Result(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }

    static Map<String, String> env(String key, String value) {
        Map<String, String> env = new LinkedHashMap<String, String>();
        env.put(key, value);
        return env;
    }
}
