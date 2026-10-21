package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's Mkdir contract against a live bucket; skipped unless OBS_LIVE_* is set. */
public class LiveS3MkdirContractTest extends AbstractContractMkdirTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLiveContract(conf);
  }
}
