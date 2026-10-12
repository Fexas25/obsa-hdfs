package org.apache.hadoop.fs.obs;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

/** Test-only SPI: exercises Hadoop provider ordering without native filesystem tools. */
public final class TestCredentialProviderFactory extends CredentialProviderFactory {
  private static final Map<String, CredentialProvider> STORES = new ConcurrentHashMap<>();

  static String register(String... pairs) {
    String uri = "obs-test-credentials:///" + UUID.randomUUID();
    Map<String, char[]> entries = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) entries.put(pairs[i], pairs[i + 1].toCharArray());
    STORES.put(uri, new MemoryProvider(entries));
    return uri;
  }

  static void remove(String uri) {
    STORES.remove(uri);
  }

  @Override
  public CredentialProvider createProvider(URI uri, Configuration conf) throws IOException {
    if (!"obs-test-credentials".equals(uri.getScheme())) return null;
    CredentialProvider provider = STORES.get(uri.toString());
    if (provider == null) throw new IOException("Unknown test credential store");
    return provider;
  }

  private static final class Entry extends CredentialProvider.CredentialEntry {
    Entry(String alias, char[] value) {
      super(alias, value.clone());
    }
  }

  private static final class MemoryProvider extends CredentialProvider {
    private final Map<String, char[]> entries;

    MemoryProvider(Map<String, char[]> entries) {
      this.entries = entries;
    }

    @Override
    public CredentialEntry getCredentialEntry(String alias) {
      char[] value = entries.get(alias);
      return value == null ? null : new Entry(alias, value);
    }

    @Override
    public List<String> getAliases() {
      return new ArrayList<>(entries.keySet());
    }

    @Override
    public CredentialEntry createCredentialEntry(String alias, char[] value) throws IOException {
      throw new IOException("Immutable test fixture");
    }

    @Override
    public void deleteCredentialEntry(String alias) throws IOException {
      throw new IOException("Immutable test fixture");
    }

    @Override
    public void flush() {}
  }
}
