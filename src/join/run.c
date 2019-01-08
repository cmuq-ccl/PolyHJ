/*
 * PolyHJ: Polymorphic Hash Join.
 * TODO: Describe File.
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

#include "common.h"

/* Function Declarations. */
void *join_thread(void*);
void  ICP(thread_t*, relation_t*, uint32_t, block_meta_t*);
void  ICP_cleanup(thread_t*);
void  ColBP_I  (thread_t*);
void  ColBP_II (thread_t*);
void  ColBP_III(thread_t*);
void  ColBP_IV (thread_t*);


/*
 * TODO: Describe Function.
 */
void execute_join() {
  uint64_t total_matches   = 0;
  uint64_t global_checksum = 0;

  /* Execute the join by Threads.N threads in parallel. */
  run_threads(join_thread);

  for(uint32_t t = 0; t < Threads.N; t++) {
    total_matches   += Threads.Args[t].matches;
    global_checksum += Threads.Args[t].checksum;
  }

  // NOTE: The value of checksum depends on whether we add up the payloads
  // or the keys of matches. Refer to value of `TEST_KEY_INPLACEOF_PAYLOAD`.
  printf("Checksum: %lu.\n",      global_checksum);
  printf("Total Matches: %lu.\n", total_matches);

  return;
}



/*
 * TODO: Describe Function.
 */
void *join_thread(void* params) {
  ttimer_t total_timer, phase_timer;
  thread_t *T   = (thread_t*)params;
  uint32_t  tid = T->tid;

  global_timer_start(&total_timer, tid);


  /* Apply ICP partitioning if the fanouts dictate so. */
  if(Radix.R > 0) {
    global_timer_start(&phase_timer, tid);

    /* Partition relation S. */
    ICP(T, T->SubS, Radix.S, &T->BlocksS);

    /* Partition relation R. */
    ICP(T, T->SubR, Radix.R, &T->BlocksR);

    global_timer_report(&phase_timer, tid, "#>> Total Partitioning");
    global_timer_start(&phase_timer, tid);
  }


  /*
   * Apply the appropriate CBP Join Model among I, II, III, IV.
   */
  if(Radix.R == Radix.S) {
    if(Radix.R == 0) ColBP_I(T);
    else             ColBP_II(T);
  }
  else {
    if(Radix.S == 0) ColBP_III(T);
    else             assert(false);
  }


  /* Report Run Time. */
  if(Radix.R > 0) {
    global_timer_report(&phase_timer, tid, "#>> Total Build/Probe");
  }

  global_timer_report(&total_timer, tid, "#>> Total Execution");

  /* Cleanup. */
  ICP_cleanup(T);

  return NULL;
}
