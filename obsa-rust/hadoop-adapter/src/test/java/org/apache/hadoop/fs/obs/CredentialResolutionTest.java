package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.junit.After;
import org.junit.Test;

public class CredentialResolutionTest {
  private final List<String> stores = new ArrayList<>();

  private String provider(String directory, String... pairs) {
    String uri = TestCredentialProviderFactory.register(pairs);
    stores.add(uri);
    return uri;
  }

  @After
  public void cleanupProviders() {
    for (String uri : stores) TestCredentialProviderFactory.remove(uri);
  }

  @Test
  public void obsProvidersPrecedeGlobalWithoutMutatingSharedConfiguration() throws Exception {
    String global =
        provider(
            "global", "fs.obs.access.key", "global-access", "fs.obs.secret.key", "global-secret");
    String local = provider("local", "fs.obs.access.key", "obs-access");
    Configuration conf = new Configuration(false);
    conf.setBoolean("hadoop.security.credential.clear-text-fallback", false);
    conf.set("fs.obs.access.key", "plain-access");
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, global);
    conf.set("fs.obs.security.credential.provider.path", " " + local + ", " + local + " ");
    Map<String, String> values = new HashMap<>();
    OBSFileSystemRust.resolveCredentials(conf, "bucket", values);
    assertEquals("obs-access", values.get("fs.obs.access.key"));
    assertEquals("global-secret", values.get("fs.obs.secret.key"));
    assertEquals(global, conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));
    assertArrayEquals("global-access".toCharArray(), conf.getPassword("fs.obs.access.key"));
  }

  @Test
  public void bucketProvidersAndLegacySpellingHaveExplicitPrecedence() throws Exception {
    String common = provider("common", "fs.obs.access.key", "common-access");
    String special =
        provider(
            "special",
            "fs.obs.access.key",
            "special-access",
            "fs.obs.bucket.bucket.secret.key",
            "bucket-secret");
    Configuration conf = new Configuration(false);
    conf.set("fs.obs.credential.provider.path", common);
    Map<String, String> values = new HashMap<>();
    OBSFileSystemRust.resolveCredentials(conf, "other", values);
    assertEquals("common-access", values.get("fs.obs.access.key"));
    conf.set("fs.obs.security.credential.provider.path", common);
    conf.set("fs.obs.bucket.bucket.credential.provider.path", special);
    values.clear();
    OBSFileSystemRust.resolveCredentials(conf, "bucket", values);
    assertEquals("special-access", values.get("fs.obs.access.key"));
    assertEquals("bucket-secret", values.get("fs.obs.bucket.bucket.secret.key"));
    conf.set("fs.obs.bucket.bucket.security.credential.provider.path", common);
    values.clear();
    OBSFileSystemRust.resolveCredentials(conf, "bucket", values);
    assertEquals("common-access", values.get("fs.obs.access.key"));
    assertFalse(values.containsKey("fs.obs.bucket.bucket.secret.key"));
    assertNull(conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH));
  }

  @Test
  public void brokenProviderDoesNotSilentlyUsePlaintext() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.obs.security.credential.provider.path", "unavailable-provider:///");
    conf.set("fs.obs.access.key", "plaintext");
    try {
      OBSFileSystemRust.resolveCredentials(conf, "bucket", new HashMap<>());
      fail("broken configured provider must fail");
    } catch (java.io.IOException expected) {
    }
  }

  @Test
  public void providerValuesOverridePlaintextIncludingBucketAliases() throws Exception {
    UserGroupInformation.createRemoteUser("isolated-credential-test")
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration(false);
                  conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "user:///");
                  conf.set("fs.obs.access.key", "plain-access");
                  conf.set("fs.obs.secret.key", "plain-secret");
                  conf.set("fs.obs.bucket.bucket.secret.key", "plain-bucket-secret");
                  CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
                  provider.createCredentialEntry(
                      "fs.obs.access.key", "protected-access".toCharArray());
                  provider.createCredentialEntry(
                      "fs.obs.secret.key", "protected-secret".toCharArray());
                  provider.createCredentialEntry(
                      "fs.obs.bucket.bucket.secret.key", "protected-bucket".toCharArray());
                  provider.createCredentialEntry(
                      "fs.obs.session.token", "protected-token".toCharArray());
                  provider.flush();
                  Map<String, String> values = new HashMap<>();
                  OBSFileSystemRust.resolveCredentials(conf, "bucket", values);
                  assertEquals("protected-access", values.get("fs.obs.access.key"));
                  assertEquals("protected-secret", values.get("fs.obs.secret.key"));
                  assertEquals("protected-bucket", values.get("fs.obs.bucket.bucket.secret.key"));
                  assertEquals("protected-token", values.get("fs.obs.session.token"));
                  // Clearing temporary password arrays must not corrupt the provider.
                  assertArrayEquals(
                      "protected-secret".toCharArray(), conf.getPassword("fs.obs.secret.key"));
                  return null;
                });
  }

  @Test
  public void disabledCleartextFallbackRemovesPreviouslyCopiedSecrets() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean("hadoop.security.credential.clear-text-fallback", false);
    Map<String, String> values = new HashMap<>();
    for (String prefix : Arrays.asList("fs.obs.", "fs.obs.bucket.bucket.")) {
      for (String suffix : Arrays.asList("access.key", "secret.key", "session.token")) {
        conf.set(prefix + suffix, "plaintext");
        values.put(prefix + suffix, "plaintext");
      }
    }
    OBSFileSystemRust.resolveCredentials(conf, "bucket", values);
    assertTrue(values.isEmpty());
  }

  @Test
  public void plaintextCompatibilityUsesHadoopFallback() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("fs.obs.access.key", "test-access");
    conf.set("fs.obs.secret.key", "test-secret");
    Map<String, String> values = new HashMap<>();
    OBSFileSystemRust.resolveCredentials(conf, "bucket", values);
    assertEquals("test-access", values.get("fs.obs.access.key"));
    assertEquals("test-secret", values.get("fs.obs.secret.key"));
    assertFalse(values.containsKey("fs.obs.session.token"));
  }
}
