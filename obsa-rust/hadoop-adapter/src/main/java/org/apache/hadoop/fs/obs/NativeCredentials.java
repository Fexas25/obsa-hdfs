package org.apache.hadoop.fs.obs;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credential providers with the Java OBSA reference's precedence:
 *
 * <ol>
 *   <li>{@code fs.obs.credentials.provider}: a {@link BasicSessionCredential} built with {@code
 *       (URI, Configuration)}; overrides configured keys.
 *   <li>Keys from configuration or a Hadoop CredentialProvider (static, resolved earlier).
 *   <li>Only when no keys are configured: {@code fs.obs.security.provider}, an OBS SDK {@code
 *       IObsCredentialsProvider} (e.g. ECS agency credentials) built with {@code (URI,
 *       Configuration)} or no arguments. It is called reflectively ({@code getSecurityKey()} then
 *       {@code getAccessKey/getSecretKey/getSecurityToken}), so the adapter needs no SDK.
 * </ol>
 *
 * <p>Unlike the reference, which reads provider 1 once, both providers are re-read every {@code
 * fs.obs.credentials.refresh.interval.ms} (default 60000; 0 disables) on a daemon thread, and
 * changed values are pushed to the native signer. A failed refresh keeps the previous values.
 */
final class NativeCredentials implements AutoCloseable {
  static final String CREDENTIALS_PROVIDER = "fs.obs.credentials.provider";
  static final String SECURITY_PROVIDER = "fs.obs.security.provider";
  static final String REFRESH_INTERVAL = "fs.obs.credentials.refresh.interval.ms";
  private static final Logger LOG = LoggerFactory.getLogger(NativeCredentials.class);

  /** Native sink for refreshed values, normally the filesystem's updateCredentials call. */
  interface Sink {
    void update(String accessKey, String secretKey, String sessionToken) throws IOException;
  }

  private final Object provider;
  private final String source;
  private String[] current;
  private ScheduledExecutorService refresher;

  private NativeCredentials(Object provider, String source, String[] initial) {
    this.provider = provider;
    this.source = source;
    this.current = initial;
  }

  /**
   * Resolve a provider and write its keys into the native properties, replacing any configured
   * plain keys. Returns null when static configuration applies.
   */
  static NativeCredentials resolve(
      URI name, Configuration conf, String bucket, Map<String, String> properties)
      throws IOException {
    Object provider;
    String source;
    Class<?> basic = providerClass(conf, CREDENTIALS_PROVIDER);
    if (basic != null) {
      provider = instantiate(basic, name, conf, false, CREDENTIALS_PROVIDER);
      if (!(provider instanceof BasicSessionCredential))
        throw new IOException(CREDENTIALS_PROVIDER + " must implement " + BasicSessionCredential.class.getName());
      source = CREDENTIALS_PROVIDER;
    } else {
      if (hasKey(properties, bucket)) return null;
      Class<?> security = providerClass(conf, SECURITY_PROVIDER);
      if (security == null) return null;
      provider = instantiate(security, name, conf, true, SECURITY_PROVIDER);
      source = SECURITY_PROVIDER;
    }
    String[] keys = read(provider, source);
    for (String prefix : Arrays.asList("fs.obs.", "fs.obs.bucket." + bucket + "."))
      for (String suffix : Arrays.asList("access.key", "secret.key", "session.token"))
        properties.remove(prefix + suffix);
    properties.put("fs.obs.access.key", keys[0]);
    properties.put("fs.obs.secret.key", keys[1]);
    if (keys[2] != null) properties.put("fs.obs.session.token", keys[2]);
    LOG.info("Using OBS credentials from {} ({})", source, provider.getClass().getName());
    return new NativeCredentials(provider, source, keys);
  }

  /** Start periodic refresh; a non-positive interval leaves the initial values in place. */
  synchronized void start(Configuration conf, Sink sink) {
    long interval = conf.getLong(REFRESH_INTERVAL, 60_000L);
    if (interval <= 0 || refresher != null) return;
    refresher =
        Executors.newSingleThreadScheduledExecutor(
            task -> {
              Thread thread = new Thread(task, "obs-native-credential-refresh");
              thread.setDaemon(true);
              return thread;
            });
    refresher.scheduleWithFixedDelay(
        () -> refresh(sink), interval, interval, TimeUnit.MILLISECONDS);
  }

  /** Re-read the provider and push changed values. Returns whether new values were pushed. */
  synchronized boolean refresh(Sink sink) {
    try {
      String[] keys = read(provider, source);
      if (Arrays.equals(keys, current)) return false;
      sink.update(keys[0], keys[1], keys[2]);
      current = keys;
      LOG.info("Refreshed OBS credentials from {}", source);
      return true;
    } catch (IOException | RuntimeException e) {
      // Never log values; keep signing with the previous credentials.
      LOG.warn("OBS credential refresh from {} failed; keeping previous credentials: {}",
          source, e.getClass().getName());
      return false;
    }
  }

  @Override
  public synchronized void close() {
    if (refresher != null) refresher.shutdownNow();
    refresher = null;
  }

  private static boolean hasKey(Map<String, String> properties, String bucket) {
    for (String key :
        Arrays.asList(
            "fs.obs.access.key", "fs.obs.secret.key",
            "fs.obs.bucket." + bucket + ".access.key", "fs.obs.bucket." + bucket + ".secret.key")) {
      String value = properties.get(key);
      if (value != null && !value.isEmpty()) return true;
    }
    return false;
  }

  private static Class<?> providerClass(Configuration conf, String key) throws IOException {
    try {
      return conf.getClass(key, null);
    } catch (RuntimeException e) {
      throw new IOException("Cannot load class from option " + key, e);
    }
  }

  private static Object instantiate(
      Class<?> type, URI name, Configuration conf, boolean allowNoArgs, String key)
      throws IOException {
    try {
      Constructor<?> constructor;
      try {
        constructor = type.getDeclaredConstructor(URI.class, Configuration.class);
        constructor.setAccessible(true);
        return constructor.newInstance(name, conf);
      } catch (NoSuchMethodException e) {
        if (!allowNoArgs) throw e;
        constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
      }
    } catch (NoSuchMethodException | IllegalAccessException | InstantiationException
        | InvocationTargetException | RuntimeException e) {
      Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
      throw new IOException("Cannot create provider from option " + key + ": " + type.getName(), cause);
    }
  }

  private static String[] read(Object provider, String source) throws IOException {
    String access, secret, token;
    if (provider instanceof BasicSessionCredential) {
      BasicSessionCredential basic = (BasicSessionCredential) provider;
      access = basic.getOBSAccessKeyId();
      secret = basic.getOBSSecretKey();
      token = basic.getSessionToken();
    } else {
      Object key = call(provider, "getSecurityKey", source);
      if (key == null) throw new IOException(source + " returned no security key");
      access = (String) call(key, "getAccessKey", source);
      secret = (String) call(key, "getSecretKey", source);
      token = (String) call(key, "getSecurityToken", source);
    }
    if (access == null || access.isEmpty() || secret == null || secret.isEmpty())
      throw new IOException(source + " returned empty access or secret key");
    return new String[] {access, secret, token == null || token.isEmpty() ? null : token};
  }

  private static Object call(Object target, String method, String source) throws IOException {
    try {
      Method m = target.getClass().getMethod(method);
      m.setAccessible(true);
      return m.invoke(target);
    } catch (InvocationTargetException e) {
      throw new IOException(source + " failed in " + method, e.getCause());
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new IOException(source + " provider lacks " + method + "()", e);
    }
  }
}
