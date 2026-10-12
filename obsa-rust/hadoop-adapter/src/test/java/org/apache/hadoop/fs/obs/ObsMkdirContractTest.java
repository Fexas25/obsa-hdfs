package org.apache.hadoop.fs.obs;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ObsMkdirContractTest extends AbstractContractMkdirTest {
  @Parameterized.Parameters(name = "pfs={0}")
  public static Collection<Object[]> modes() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private final boolean pfs;

  public ObsMkdirContractTest(boolean pfs) {
    this.pfs = pfs;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLocalContract(conf, pfs);
  }
}
