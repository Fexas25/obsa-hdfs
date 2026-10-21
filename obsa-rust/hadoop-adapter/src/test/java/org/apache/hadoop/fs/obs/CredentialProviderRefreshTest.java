package org.apache.hadoop.fs.obs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class CredentialProviderRefreshTest {
  /** fs.obs.credentials.provider implementation with rotatable values. */
  public static final class RotatingBasic implements BasicSessionCredential {
    static volatile String access = "AK-basic-1", secret = "SK-basic-1", token = "TOKEN-basic-1";
    static volatile URI seenUri;

    public RotatingBasic(URI name, Configuration conf) {
      seenUri = name;
    }

    @Override
    public String getOBSAccessKeyId() {
      return access;
    }

    @Override
    public String getOBSSecretKey() {
      return secret;
    }

    @Override
    public String getSessionToken() {
      return token;
    }
  }

  /** Shape of an OBS SDK ISecurityKey, used reflectively. */
  public static final class Key {
    private final String access, secret, token;

    Key(String access, String secret, String token) {
      this.access = access;
      this.secret = secret;
      this.token = token;
    }

    public String getAccessKey() {
      return access;
    }

    public String getSecretKey() {
      return secret;
    }

    public String getSecurityToken() {
      return token;
    }
  }

  /** Shape of an OBS SDK IObsCredentialsProvider with a no-argument constructor. */
  public static final class RotatingSecurity {
    static volatile Key key = new Key("AK-ecs-1", "SK-ecs-1", "TOKEN-ecs-1");
    static volatile boolean fail;

    public Key getSecurityKey() {
      if (fail) throw new IllegalStateException("metadata service unavailable");
      return key;
    }
  }

  private static void touch(OBSFileSystemRust fs) throws IOException {
    fs.exists(new Path("/probe-" + System.nanoTime()));
  }

  @Test
  public void basicSessionCredentialOverridesConfiguredKeysAndRefreshes() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(NativeCredentials.CREDENTIALS_PROVIDER, RotatingBasic.class.getName());
    conf.setLong(NativeCredentials.REFRESH_INTERVAL, 0); // refreshed explicitly below
    RotatingBasic.access = "AK-basic-1";
    RotatingBasic.token = "TOKEN-basic-1";
    ObsLocalContract contract = new ObsLocalContract(conf, false);
    contract.init();
    try {
      OBSFileSystemRust fs = (OBSFileSystemRust) contract.getTestFileSystem();
      LocalObsServer server = contract.server();
      assertEquals(URI.create("obs://bucket"), RotatingBasic.seenUri);
      touch(fs);
      assertEquals("AK-basic-1", server.lastAccessKey);
      assertEquals("TOKEN-basic-1", server.lastSecurityToken);

      assertFalse("unchanged values are not pushed", fs.refreshCredentials());
      RotatingBasic.access = "AK-basic-2";
      RotatingBasic.token = "TOKEN-basic-2";
      assertTrue(fs.refreshCredentials());
      touch(fs);
      assertEquals("AK-basic-2", server.lastAccessKey);
      assertEquals("TOKEN-basic-2", server.lastSecurityToken);

      // An invalid rotation is rejected and the previous credentials stay in use.
      RotatingBasic.access = "";
      assertFalse(fs.refreshCredentials());
      touch(fs);
      assertEquals("AK-basic-2", server.lastAccessKey);
    } finally {
      contract.teardown();
      RotatingBasic.access = "AK-basic-1";
    }
  }

  @Test
  public void keylessSecurityProviderIsUsedReflectivelyAndRefreshedPeriodically() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(NativeCredentials.SECURITY_PROVIDER, RotatingSecurity.class.getName());
    conf.setLong(NativeCredentials.REFRESH_INTERVAL, 50);
    RotatingSecurity.key = new Key("AK-ecs-1", "SK-ecs-1", "TOKEN-ecs-1");
    RotatingSecurity.fail = false;
    ObsLocalContract contract = new ObsLocalContract(conf, true);
    contract.init();
    try {
      OBSFileSystemRust fs = (OBSFileSystemRust) contract.getTestFileSystem();
      LocalObsServer server = contract.server();
      touch(fs);
      assertEquals("AK-ecs-1", server.lastAccessKey);
      assertEquals("TOKEN-ecs-1", server.lastSecurityToken);

      // A failing provider keeps the old credentials.
      RotatingSecurity.fail = true;
      Thread.sleep(200);
      touch(fs);
      assertEquals("AK-ecs-1", server.lastAccessKey);

      // The background refresher picks up rotated values; permanent keys drop the token.
      RotatingSecurity.key = new Key("AK-ecs-2", "SK-ecs-2", null);
      RotatingSecurity.fail = false;
      long deadline = System.nanoTime() + 5_000_000_000L;
      do {
        Thread.sleep(20);
        touch(fs);
      } while (!"AK-ecs-2".equals(server.lastAccessKey) && System.nanoTime() < deadline);
      assertEquals("AK-ecs-2", server.lastAccessKey);
      assertNull(server.lastSecurityToken);
    } finally {
      contract.teardown();
    }
  }

  @Test
  public void configuredKeysWinOverSecurityProvider() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(NativeCredentials.SECURITY_PROVIDER, RotatingSecurity.class.getName());
    conf.set("fs.obs.access.key", "AK-static");
    conf.set("fs.obs.secret.key", "SK-static");
    ObsLocalContract contract = new ObsLocalContract(conf, false);
    contract.init();
    try {
      OBSFileSystemRust fs = (OBSFileSystemRust) contract.getTestFileSystem();
      touch(fs);
      assertEquals("AK-static", contract.server().lastAccessKey);
      assertFalse(fs.refreshCredentials());
    } finally {
      contract.teardown();
    }
  }

  @Test
  public void invalidProvidersFailInitialization() throws Exception {
    for (String[] option :
        new String[][] {
          {NativeCredentials.CREDENTIALS_PROVIDER, RotatingSecurity.class.getName()},
          {NativeCredentials.CREDENTIALS_PROVIDER, "no.such.Provider"},
          {NativeCredentials.SECURITY_PROVIDER, String.class.getName()},
        }) {
      Configuration conf = new Configuration(false);
      conf.set(option[0], option[1]);
      ObsLocalContract contract = new ObsLocalContract(conf, false);
      try {
        IOException error = assertThrows(IOException.class, contract::init);
        assertTrue(error.getMessage(), error.getMessage().contains(option[0]));
      } finally {
        contract.teardown();
      }
    }
  }
}
