/*
 * PolyHJ: Polymorphic Hash Join.
 * Collaborative Building and Probing (ColBP) Procedures, Model I.
 * TODO: More info about model.
 * (Refer to note in run.c w.r.t. ColBP models)
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include "common.h"


void ColBP_I(thread_t* T) { assert(Radix.R == 0 && Radix.S == 0);
  ttimer_t phase_timer;
  uint64_t matches = 0, checksum = 0;
  uint32_t tid = T->tid;

  global_timer_start(&phase_timer, tid);

  /* Allocate and NUMA-distribute shared Hash Table. */
  uint32_t HTable_size = Threads.RelR->size + 1;

  if(tid == 0) {
    Threads.HTables    = SafeMalloc(sizeof(bucket_t*));
    Threads.HTables[0] = SafeMalloc(HTable_size * sizeof(bucket_t));
  }

  barrier(); // Wait for allocation.

  bucket_t *HTable = Threads.HTables[0];

  uint32_t share   = HTable_size / Threads.N;
  uint32_t offset = tid * share;
  memset(HTable + offset, 0, share * sizeof(bucket_t));

  barrier(); // Wait for NUMA distribution.

  /* Build HTable from R. */
  tuple_t *R     = T->SubR->tuples;
  uint32_t sizeR = T->SubR->size;

  for(uint32_t i = 0; i < sizeR; i++) {
    tuple_t t = R[i];
    tkey_t  k = t.key;

    /* Scatter, NOPA-style Array-based. */
    #if !TEST_KEY_INPLACEOF_PAYLOAD
      HTable[k] = t.payload;
    #else
      HTable[k] = k;
    #endif

    checksum += k;
  }

  barrier(); // Wait for completely constructed table.

  global_timer_report(&phase_timer, tid, "#>> Total Building");
  global_timer_start(&phase_timer, tid);


  /* Probe HTable from S. */
  tuple_t *S     = T->SubS->tuples;
  uint32_t sizeS = T->SubS->size;

  for(uint32_t i = 0; i < sizeS; i++) {
    tuple_t t = S[i];
    tkey_t  k = t.key;

    /*
     * Gather, NOPA-style Array-based.
     * For comparability with previous work (e.g., Balkesen et al., Kim et al.,
     * Schuh et al.'s main experiments), the join result is not materialized.
     * Rather, we locate and access the matches' payloads.
     */
    checksum += HTable[k];

    #if !TEST_KEY_INPLACEOF_PAYLOAD
      ++matches;
    #else
      if(HTable[k] == k) ++matches;
    #endif
  }

  // NOTE: global_timer_report() contains (a necessary) barrier().
  // If this call is removed for any reason, a call to `barrier()` must be
  // re-placed here, before "cleanup" is applied.
  global_timer_report(&phase_timer, tid, "#>> Total Probing");

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
