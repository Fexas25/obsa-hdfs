package org.apache.hadoop.fs.obs;

import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.util.DataChecksum;

/**
 * Builds the COMPOSITE-CRC32C checksum (Hadoop 3.1+). Kept out of OBSFileSystemRust: bytecode
 * verification of a method returning a CompositeCrcFileChecksum as a FileChecksum loads that class,
 * so referencing it directly would stop the filesystem class from loading on older releases.
 */
final class NativeChecksums {
  private NativeChecksums() {}

  static FileChecksum compositeCrc32c(int crc) {
    return new CompositeCrcFileChecksum(crc, DataChecksum.Type.CRC32C, -1);
  }
}
