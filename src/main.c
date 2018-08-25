/*
 * PolyHJ: Polymorphic Hash Join.
 * > Defines and initializes `Threads` and `Radix` global structures.
 * > Initializes default join parameters.
 * > Parses input arguments (via command line) to overwrite default parameters.
 *    # For more information about the arguments, refer to `util/cmd_args.c`.
 * > Generates the (random) input relations R and S.
 * > Runs PolyHJ, with automatic parameter selection (unless provided radices).
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

#include "common.h"
#include "util/sys_info.h"
#include "types.h"

/* Global Variables. */
params_t          Threads;
radix_info_t      Radix;

/* Helper Function(s) Declarations. */
void  extract_cmd_args(int, char **);
void  prepare_threads_meta();
void  prepare_threads_meta_cleanup();
void *create_R(void*);
void *create_S(void*);
void  execute_join();
void  create_rel_cleanup();


int main(int argc, char **argv) {
  relation_t RelR, RelS;

  /* Obtain System Information. */
  sys_info_prepare();

  /* Prepare meta-data of relations. */
  Threads.RelR = &RelR;  Threads.RelS = &RelS;
  RelR.id      = 'R';    RelS.id      = 'S';
  RelR.seed    = 12345;  RelS.seed    = 54321;

  /* Set default values. */
  Threads.N = SysInfo.num_cpus; // By default, run on all CPUs.
  RelR.size = 128*1000*100;     // 12.8M 8-byte tuples
  RelS.size = 128*1000*100;     // 12.8M
  RelS.skew = 0.0;              // uniform distribution
  Threads.favor_physical_cores = true; // See meaning in ``util/threads.c``

  /* Extract command line parameters. */
  extract_cmd_args(argc, argv);

  /* Assign each thread a CPU, and populate thread information. */
  prepare_threads_meta();

  /*
   * Calculate Radix.R and set the _initial_ Radix.S accordingly.
   *
   * For very small input (up to a small multiple of LLC size), run Model I.
   * Otherwise, run Model II/III with f_R based on a large fraction of LLC size.
   */
  uint32_t ratiox = sizeof(bucket_t) * RelR.size / (SysInfo.llc_size * 6 / 5);
  uint32_t ratio  = sizeof(bucket_t) * RelR.size / (SysInfo.llc_size * 2 / 3);
  if(Radix.user_defined == false && ratiox >= 1)
    Radix.R = Radix.S = lg_ceil(ratio);

  /* NOTE.
   * Skew estimation, and the potential selection of Model III, occur as
   * (an initial) part of the ICP partitioning procedure of relation S.
   * For more information, refer to `join/partition.c`.
   */

  /* Print Configuration Info. */
  printf("Join Info: |R| = %u, |S| = %u (z = %.2f), f_R = 2^%d, f_S ~= 2^%d.\n",
         RelR.size, RelS.size, RelS.skew, Radix.R, Radix.S);

  /* Print threads/CPU mapping info. */
  printf("Running %d threads, pinned to %d "
         "hyperthread(s)/core on %d LLC(s) [%.2f MiBs each].\n",
         Threads.N, Threads.utilized_cpus_per_core,
         Threads.utilized_llcs, SysInfo.llc_size/1024.0/1024.0);

  /* Generate input relations R and S. */
  double mbs;
  mbs = sizeof(tuple_t) * RelR.size/1024.0/1024.0;
  printf("Creating R [%.2f MiBs]. ", mbs); fflush(stdout);
  run_threads(create_R);

  mbs = sizeof(tuple_t) * RelS.size/1024.0/1024.0;
  printf("Creating S [%.2f MiBs]. ", mbs); fflush(stdout);
  run_threads(create_S);
  puts("Done.");


  /* Run PolyHJ. */
  execute_join();

  /* Cleanup. */
  create_rel_cleanup();
  prepare_threads_meta_cleanup();
  sys_info_cleanup();

  return 0;
}
