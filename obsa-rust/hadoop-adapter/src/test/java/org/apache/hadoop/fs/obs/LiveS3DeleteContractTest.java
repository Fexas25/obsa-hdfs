package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's Delete contract against a live bucket; skipped unless OBS_LIVE_* is set. */
public class LiveS3DeleteContractTest extends AbstractContractDeleteTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLiveContract(conf);
  }
}
