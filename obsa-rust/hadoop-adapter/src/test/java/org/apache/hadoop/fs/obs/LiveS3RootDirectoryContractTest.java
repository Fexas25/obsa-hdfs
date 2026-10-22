package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Hadoop's root-directory contract against a live bucket. It deletes everything in the bucket, so
 * it runs only with OBS_LIVE_ROOT_TESTS=true on a dedicated, empty test bucket.
 */
public class LiveS3RootDirectoryContractTest extends AbstractContractRootDirectoryTest {
  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new ObsLiveContract(conf);
  }
}
