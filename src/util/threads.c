/*
 * PolyHJ: Polymorphic Hash Join.
 *
 * (a) prepare_threads_meta()
 * (b) prepare_threads_meta_cleanup()
 * (c) run_threads()
 */

#ifndef _GNU_SOURCE
  #define _GNU_SOURCE /* CPU_SET(), CPU_ZERO(), pthread_attr_setaffinity_np() */
#endif

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sched.h>
#include "common.h"


/*
 * Prepares and popluates Threads.Args, assigning each thread a CPU.
 *
 * If the --favor_hyperthreading flag is passed (or the default is changed
 * from ``main.c``), then Threads.favor_physical_cores would be false.
 * Otherwise, it's true by default.
 *
 * If true, and enough physical cores exist on the host machine to satisfy
 * all required threads, then each thread is pinned to a distinct physical
 * core. If not enough physcial cores exist, hyperthreading must be used,
 * and it is used on as few LLCs as possible.
 *
 * If false, and enough cores/hyperthreads exist on x LLC(s), then only
 * x LLC(s) are used, with hyperthreading (only) if necessary.
 *
 * NOTE: Rejects Thread.N > Available # of CPUs, since it tries to pin each
 *       thread to a unique hardware context. Perhaps, TODO, accept such input.
 *
 */
void prepare_threads_meta() {
  uint32_t cpus_per_core = SysInfo.cpus_per_core;
  uint32_t cores_per_llc = SysInfo.cores_per_llc;
  uint32_t cpus_per_llc  = cpus_per_core * cores_per_llc;

  // Limit (utilized) ``cpus_per_core`` to one, if conditions are satisfied.
  if(Threads.favor_physical_cores && SysInfo.num_cores >= Threads.N) {
    cpus_per_core = 1;
    cpus_per_llc  = cores_per_llc;
  }

  // Determine the minimum sufficient number of LLCs to run CPUs on.
  uint32_t utilized_llcs = div_ceil(Threads.N, cpus_per_llc);

  // Determine the number of (hyper)threads to be run on each core.
  uint32_t utilizable_cores       = utilized_llcs * cores_per_llc;
  uint32_t utilized_cpus_per_core = div_ceil(Threads.N, utilizable_cores);

  if(utilized_llcs > SysInfo.num_llcs) {
    printf("Warning: Cannot run with %d threads.\n", Threads.N);
    printf("On this machine, the driver supports up to %d threads.\n",
           SysInfo.num_llcs * SysInfo.cores_per_llc * SysInfo.cpus_per_core);
    if(Threads.N <= SysInfo.num_cpus)
      puts("Possible Reason: Different # of cores/contexts on different LLCs?");
    abort();
  }

  /* Prepare and populate the Threads.Args array. */
  Threads.Args          = SafeCalloc(Threads.N, sizeof(thread_t));
  Threads.num_groups    = utilized_llcs;
  Threads.utilized_llcs = utilized_llcs;
  Threads.utilized_cpus_per_core = utilized_cpus_per_core;

  // Data about mapping threads to CPUs.
  uint32_t  llc = 0;                        // ID of current LLC.
  uint32_t cores_on_llc[utilized_llcs];     // # of cores used so far on LLC.
  uint32_t cpus_on_core[SysInfo.num_cores]; // # of CPUs used so far on core.

  memset(cores_on_llc, 0, utilized_llcs     * sizeof(uint32_t));
  memset(cpus_on_core, 0, SysInfo.num_cores * sizeof(uint32_t));

  // Data about thread's shares from each relation.
  relation_t *RelR           = Threads.RelR;
  relation_t *RelS           = Threads.RelS;
  uint32_t R_section         = RelR->size / Threads.N;
  uint32_t S_section         = RelS->size / Threads.N;
  const uint32_t R_remainder = RelR->size % Threads.N;
  const uint32_t S_remainder = RelS->size % Threads.N;
  uint32_t R_leftover        = R_remainder;
  uint32_t S_leftover        = S_remainder;

  assert(Threads.N <= SysInfo.num_cpus);

  // For each thread, populate values and assign a CPU.
  for(uint32_t t = 0; t < Threads.N; t++) {
    thread_t *T = Threads.Args + t;

    // Set basic meta-data.
    T->tid  = t;
    T->SubR = SafeMalloc(sizeof(relation_t));
    T->SubS = SafeMalloc(sizeof(relation_t));
    T->SubR->id = 'R'; T->SubS->id = 'S';
    T->SubR->offset = t*R_section + ( R_remainder - R_leftover );
    T->SubS->offset = t*S_section + ( S_remainder - S_leftover );
    T->SubR->size   = R_section + (R_leftover > 0 && R_leftover--);
    T->SubS->size   = S_section + (S_leftover > 0 && S_leftover--);

    // Pick a CPU and set LLC group data.
    core_t *core = SysInfo.LLCs[llc].Cores[ cores_on_llc[llc] ];

    T->group = llc;
    T->CPU   = core->CPUs[ cpus_on_core[core->id]++ ];

    // If we have already placed enough (hyper)threads on current core,
    if(cpus_on_core[core->id] == utilized_cpus_per_core) {
      // Then, later, continue with next available core on the current LLC.
      cores_on_llc[llc]++;
    }

    // Let the next thread be on the "next" (utilized) LLC.
    llc = (llc + 1) % utilized_llcs;

    /* Print Thread-CPU mapping. */
    /*
    printf("[T%d: %d,%d,%d] ", t, T->group, T->CPU->core, T->CPU->id);
    if(t+1 == Threads.N || t % 7 == 6) puts("");
    */
  }


  /* Initialize the threads' barriers. */
  barrier_init();

  return;
}


/*
 * Cleanup for prepare_threads_meta() by free()`ing its allocations.
 */
void prepare_threads_meta_cleanup() {
  for(uint32_t t = 0; t < Threads.N; t++) {
    free(Threads.Args[t].SubR);
    free(Threads.Args[t].SubS);
  }

  free(Threads.Args);
}


/*
 * (a) Runs `Threads.N` threads with f((void*) Args), assigning each thread to
 * its respective CPU.
 * (b) Waits for all threads to complete execution, before returning.
 */
void run_threads(void* (*f) (void*)) {
  cpu_set_t      set;
  pthread_attr_t attr;
  pthread_t     *tids = calloc(Threads.N, sizeof(pthread_t));

  for(uint32_t t = 0; t < Threads.N; t++) {
    thread_t *T = Threads.Args + t;

    pthread_attr_init(&attr);
    CPU_ZERO(&set);
    CPU_SET(T->CPU->id, &set);

    /* Create thread, pinned to CPU at T->CPU. */
    assert( pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set) == 0 );
    assert( pthread_create(tids + t, &attr, f, (void*)T) == 0 );

    pthread_attr_destroy(&attr); // Destroying attr has no effect on thread.
  }

  /* Wait for all threads to complete execution, ignoring return values. */
  for(int t = 0; t < Threads.N; t++) pthread_join(tids[t], NULL);

  /* Cleanup. */
  free(tids);

  return;
}
