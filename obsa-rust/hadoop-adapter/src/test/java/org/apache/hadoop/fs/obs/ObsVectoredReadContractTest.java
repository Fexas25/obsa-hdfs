package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's vectored read contract (direct and heap buffers) over the native stack. */
public class ObsVectoredReadContractTest extends AbstractContractVectoredReadTest {
  public ObsVectoredReadContractTest(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    // Small readahead chunks so merged spans are split across several ranged GETs.
    conf.set("fs.obs.readahead.chunk.size", "4096");
    return new ObsLocalContract(conf, false);
  }
}
