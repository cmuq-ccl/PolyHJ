/*
 * PolyHJ: Polymorphic Hash Join.
 * Collaborative Building and Probing (ColBP) Procedures, Model II.
 * TODO: More info about model.
 * (Refer to note in run.c w.r.t. ColBP models)
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include "common.h"


void ColBP_II(thread_t* T) { assert(Radix.R == Radix.S && Radix.R > 0);
  uint64_t matches = 0, checksum = 0;

  /* Thread Data. */
  uint32_t tid        = T->tid;
  uint32_t group      = T->group;
  uint32_t num_groups = Threads.num_groups;
  assert(tid % num_groups == group); // expected from prepare_threads_meta()

  /* Sub-Relations. */
  tuple_t *R = T->SubR->tuples;
  tuple_t *S = T->SubS->tuples;

  /* ICP "Blocks" Data. */
  block_t **BlocksR      = T->BlocksR.Pos;
  block_t **BlocksS      = T->BlocksS.Pos;
  uint32_t  num_blocks_R = T->BlocksR.N;
  uint32_t  num_blocks_S = T->BlocksS.N;

  /*
   * Allocate and NUMA-distribute Hash Table(s).
   * Given threads lie on x LLCs, allocate x hash tables.
   */
  uint32_t avg_partition = (Threads.RelR->size >> Radix.R) + 1;
  uint32_t HTable_size   = 1 << lg_ceil(avg_partition);

  // Thread zero allocates list of tables.
  if(tid == 0) Threads.HTables = SafeMalloc(num_groups * sizeof(bucket_t*));

  barrier(); // Wait for allocation.

  // One thread from each group allocates one table.
  if(tid == group) { // (relies on assertion `tid % num_groups == group`)
    Threads.HTables[group] = SafeMalloc(HTable_size * sizeof(bucket_t));
  }

  barrier(); // Wait for allocation(s).

  // NUMA-distribute each table.
  for(uint32_t g = 0; g < num_groups; g++) {
    uint32_t t      = num_groups * 2; // 2 threads per group (arbitrary).
    bucket_t *Table = Threads.HTables[g];
    uint32_t share  = HTable_size / t;
    uint32_t offset = tid * share;

    if(tid < t) memset(Table + offset, 0, share * sizeof(bucket_t));
  }

  barrier(); // Wait for NUMA distribution.


  /*
   * Cooperative Iterations of Build/Probe.
   * TODO: Implement support for cases where FanoutR % num_groups != 0.
   *       The solution is basically a loop starting at (iters * num_groups)
   *       with condition (i < FanoutR), in which the remainder partitions
   *       are handled using a single, shared hash table.
   *       This requires sub-blocks prepared appropriately in ICP.
   */
  uint32_t iters           = FanoutR / num_groups;
  uint32_t remainder_iters = FanoutR % num_groups;
  assert(remainder_iters == 0);

  for(uint32_t i = 0; i < iters; i++) {
    /*
     * Build Phase.
     * Each group of threads (sharing an LLC) scatter to a separate hash table,
     * while the other threads are scattering to other hash tables (for other
     * partitions). Then, the groups swap hash tables and partitions.
     */
    for(uint32_t g = 0; g < num_groups; g++) {
      uint32_t h       = (g + group) % num_groups; // Hash Table index.
      uint32_t p       = h * iters + i;            // Partition index.
      bucket_t *HTable = Threads.HTables[h];       // Hash Table.

      /* Scan partitions (chunked across blocks) and Scatter. */
      for(uint32_t b = 0; b < num_blocks_R; b++) {
        uint32_t idx   = BlocksR[b][h].start;
        uint32_t end   = BlocksR[b][h].end;
        uint32_t radix = Radix.R;
        uint32_t mask  = MaskR;

        for(; idx < end && p == HASH(R[idx].key, mask); idx++) {
          tuple_t t = R[idx];
          tkey_t  k = t.key;

          /* Scatter, CPRA-style Array-based. */
          #if !TEST_KEY_INPLACEOF_PAYLOAD
            HTable[k >> radix] = t.payload;
          #else
            HTable[k >> radix] = k;
          #endif

          checksum += k;
        }

        BlocksR[b][h].start = idx; // Update index within sub-block.
      }

      sbarrier(tid); // Synchronize swapping hash tables across groups.
      // NOTE: This is not necessary for correctness, but may contribute
      // to performance. Essentially, it reduces cross-LLC false sharing.
      // Yet, of course, at least one barrier must be present before probing.
    }



    /*
     * Probe Phase.
     * Like the build phase, but without barriers.
     */
    for(int g = num_groups - 1; g >= 0; g--) {
      uint32_t h       = (g + group) % num_groups; // Hash Table index.
      uint32_t p       = h * iters + i;            // Partition index.
      bucket_t *HTable = Threads.HTables[h];       // Hash Table.

      /* Scan partitions (chunked across blocks) and Gether. */
      for(uint32_t b = 0; b < num_blocks_S; b++) {
        uint32_t idx   = BlocksS[b][h].start;
        uint32_t end   = BlocksS[b][h].end;
        uint32_t shift = Radix.R;
        uint32_t mask  = MaskS;

        for(; idx < end && p == HASH(S[idx].key, mask); idx++) {
          tuple_t t = S[idx];
          tkey_t  k = t.key;

          /*
           * Gather, CPRA-style Array-based.
           * For comparability with previous work (e.g., Balkesen et al.,
           * Kim et al., Schuh et al.'s main experiments), the join result is
           * not materialized. Rather, we locate and access the
           * matches' payloads.
           */
          checksum += HTable[k >> shift];

          #if !TEST_KEY_INPLACEOF_PAYLOAD
            ++matches;
          #else
            if(HTable[k >> shift] == k) ++matches;
          #endif
        }

        BlocksS[b][h].start = idx; // Update index within sub-block.
      }
    }


    sbarrier(tid); // Avoid building for new partitions until probing is done.
  }


  /* Set thread-local matches and checksum. */
  T->matches  = matches;
  T->checksum = checksum;

  /*
   * Cleanup.
   * This cleanup should not occur until all probing is complete.
   * For Model II, there is a barrier after each probing iteration, so that
   * suffices.
   */
  if(tid == group) free(Threads.HTables[group]);
  if(tid == 0)     free(Threads.HTables);

  return;
}
