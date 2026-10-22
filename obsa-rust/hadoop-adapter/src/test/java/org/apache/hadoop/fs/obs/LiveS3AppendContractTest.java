package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractAppendTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's Append contract (runs only with OBS_LIVE_CONTRACT_PFS=true; object buckets cannot append) against a live bucket; skipped unless OBS_LIVE_* is set. */
public class LiveS3AppendContractTest extends AbstractContractAppendTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLiveContract(conf);
  }
}
