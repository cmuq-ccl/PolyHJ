/*
 * PolyHJ: Polymorphic Hash Join.
 * Collaborative Building and Probing (ColBP) Procedures, Model III.
 * TODO: More info about model.
 * (Refer to note in run.c w.r.t. ColBP models)
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include "common.h"

/* Global Variable(s). */
extern uint8_t ModelIII_shift;


void ColBP_III(thread_t* T) { assert(Radix.R > 0 && Radix.S == 0);
  uint64_t matches = 0, checksum = 0;

  /* Thread Data. */
  uint32_t tid        = T->tid;
  uint32_t group      = T->group;
  uint32_t num_groups = Threads.num_groups;
  assert(tid % num_groups == group); // expected from prepare_threads_meta()

  /* Sub-Relations and ICP "Blocks" Data (applicable only for R). */
  tuple_t *R             = T->SubR->tuples;
  tuple_t *S             = T->SubS->tuples;
  uint32_t sizeS         = T->SubS->size;
  block_t **BlocksR      = T->BlocksR.Pos;
  uint32_t  num_blocks_R = T->BlocksR.N;

  /*
   * Allocate FanoutR Hash Tables (in one aggregate space).
   * NUMA-distribution of regions in the aggregate hash table is achieved
   * naturally by how the build phase proceeds in Model III.
   */
  uint32_t HTable_size   = Threads.RelR->size + 1;
  if(tid == 0) {
    Threads.HTables    = SafeMalloc(sizeof(bucket_t*));
    Threads.HTables[0] = SafeMalloc(HTable_size * sizeof(bucket_t));
  }


  barrier(); // Wait until allocation is done.

  /*
   * Cooperative Build Phase Iterations.
   * Model III requires all hash tables for all R-partitions to be constructed
   * completely, before the probe phase can start from unsliced relation S.
   * TODO: Implement support for FanoutR % num_groups != 0. See buildprobe_II.c
   */
  bucket_t *GlobalTable    = Threads.HTables[0];
  uint32_t iters           = FanoutR / num_groups;
  uint32_t remainder_iters = FanoutR % num_groups;
  assert(remainder_iters == 0);

  for(uint32_t i = 0; i < iters; i++) {
    /*
     * Each group of threads (sharing an LLC) scatter to a separate hash table,
     * while the other threads are scattering to other hash tables (for other
     * partitions). Then, the groups swap hash tables and partitions.
     */
    for(uint32_t g = 0; g < num_groups; g++) {
      uint32_t h       = (g + group) % num_groups; // Hash Table index.
      uint32_t p       = h * iters + i;            // Partition index.

      /* Scan partitions (chunked across blocks) and Scatter. */
      for(uint32_t b = 0; b < num_blocks_R; b++) {
        uint32_t idx   = BlocksR[b][h].start;
        uint32_t end   = BlocksR[b][h].end;
        uint32_t shift = ModelIII_shift;
        uint32_t mask  = MaskR;

        for(; idx < end && p == HASHx(R[idx].key, mask, shift); idx++) {
          tuple_t t = R[idx];
          tkey_t  k = t.key;

          /* Scatter, NOPA/CPRA-style Array-based. */
          #if !TEST_KEY_INPLACEOF_PAYLOAD
            GlobalTable[k] = t.payload;
          #else
            GlobalTable[k] = k;
          #endif

          checksum += k;
        }

        BlocksR[b][h].start = idx; // Update index within sub-block.
      }

      sbarrier(tid); // Synchronize swapping hash tables across groups.
    }
  }


  barrier(); // Wait until all tables are constructed. [Actually, is redundant!]


  /* Cooperative Probe Phase. */
  for(uint32_t i = 0; i < sizeS; i++) {
    tuple_t t = S[i];
    tkey_t  k = t.key;

    /*
     * Gather, NOPA/CPRA-style Array-based.
     * For comparability with previous work (e.g., Balkesen et al., Kim et al.,
     * Schuh et al.'s main experiments), the join result is not materialized.
     * Rather, we locate and access the matches' payloads.
     */
    checksum += GlobalTable[k];

    #if !TEST_KEY_INPLACEOF_PAYLOAD
      ++matches;
    #else
      if(GlobalTable[k] == k) ++matches;
    #endif
  }

  barrier(); // Wait until all probing is done (before cleanup).


  /* Set thread-local matches and checksum. */
  T->matches  = matches;
  T->checksum = checksum;

  /* Cleanup. */
  if(tid == 0) {
    free(Threads.HTables[0]);
    free(Threads.HTables);
  }

  return;
}
