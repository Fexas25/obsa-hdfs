package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's vectored read contract against a live bucket; skipped unless OBS_LIVE_* is set. */
public class LiveS3VectoredReadContractTest extends AbstractContractVectoredReadTest {
  public LiveS3VectoredReadContractTest(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLiveContract(conf);
  }
}
