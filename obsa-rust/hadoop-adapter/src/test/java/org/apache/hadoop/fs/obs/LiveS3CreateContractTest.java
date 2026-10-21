package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's Create contract against a live bucket; skipped unless OBS_LIVE_* is set. */
public class LiveS3CreateContractTest extends AbstractContractCreateTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLiveContract(conf);
  }
}
