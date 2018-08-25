/*
 * PolyHJ: Polymorphic Hash Join.
 *
 * SysInfo provides a relatively detailed description of the host machine
 * in terms of LLC capacity and line size, VM page size, and most importantly
 * a hierarchicy of LLC(s) > Physical Core(s) > Hardware Thread(s)/CPU(s).
 *
 * Usage:  To initialize, call sys_info_prepare() with no arguments.
 *         To finalize, call sys_info_cleanup() with no arguments.
 *         Once initialized, all output information is populated into
 *         global `SysInfo`.
 *         For detailed information about types, refer to ``util/sys_info.h``.
 *
 * TODO: Find reliable way to determine VM page size (w/ huge pages awareness).
 *       > Refer to note above function sys_info_prepare().
 *       > Potential options are via cat'ing
 *         # /sys/kernel/mm/transparent_hugepage/enabled
 *         # /proc/meminfo
 *       > getconf does not seem very reliable for this purpose, but may serve
 *         as a good fallback.
 *
 * TODO: Substitute m/calloc() -> SafeM/Calloc(), realloc() -> SafeRealloc().
 *       > To do so, SafeRealloc() needs to be written first (within util.c).
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>

#include "common.h"
#include "util/sys_info.h"


/* Global System Information; set by current file. */
sys_info_t SysInfo;

/* Helper Functions Declarations. */
void prepare_llc_info();
bool prepare_sys_hierarchy();


/* Sets SysInfo.* and populates SysInfo.LLCs and its child Cores and CPUs.
 * On Failure to obtain SysInfo.llc_size, it aborts.
 * On Failure to properly populate SysInfo.LLCs, it aborts.
 *
 * Due to observed issues with asking the kernel for page size,
 * the page size is (conservatively) set to 2MiBs by default (not 4KiBs).
 */
void sys_info_prepare() {
  /*
   * Defaults (relied upon in case they cannot be automatically determined).
   * Edit the items below to non-zero values to manually set them.
   */
  SysInfo.page_size = 2*1024*1024; // bytes.
  SysInfo.llc_level = 0; // set to 1, 2 or 3 for L1, L2 or L3 being last-level.
  SysInfo.llc_size  = 0; // bytes.
  SysInfo.line_size = 0; // bytes.

  /* Attempt to set SysInfo.llc_level, SysInfo.llc_size, SysInfo.line_size. */
  prepare_llc_info();

  // If LLC size is unknown (which would be true if LLC level is unkown too),
  if(SysInfo.llc_size == 0) {
    puts("Error: Unable to automatically extract LLC capacity.");
    printf("Info:  You can manually set values in ``%s``\n", __FILE__);
    abort();
  }

  // If LLC cache line size is unknown, print error message but set it anyway.
  if(SysInfo.line_size == 0) {
    SysInfo.line_size = 64; // bytes.
    puts("Error: Unable to automatically extract cache line size.");
    printf("Info:  Using default value of %ld bytes.\n", SysInfo.line_size);
    printf("Info:  You can manually set values in ``%s``\n", __FILE__);
  }

  /* Attempt to set SysInfo.num_llcs and populate SysInfo.LLCs. */
  if(!prepare_sys_hierarchy()) {
    puts("Error: Unable to extract cache and CPU hierarchicy.");
    printf("Info:  You may want to manually populate this info. at ``%s``",
           __FILE__);
    abort();
  }

  return;
}


void sys_info_cleanup() {
  for(uint32_t i = 0; i < SysInfo.num_cores; i++) free(SysInfo.Cores[i].CPUs);
  for(uint32_t i = 0; i < SysInfo.num_llcs; i++)  free(SysInfo.LLCs[i].Cores);

  free(SysInfo.CPUs);
  free(SysInfo.Cores);
  free(SysInfo.LLCs);
}


/* Sets SysInfo.llc_level, SysInfo.llc_size and SysInfo.line_size.
 * On failure to obtain a value, the corresponding item is untouched.
 */
void prepare_llc_info() {
  char cmdline[LINEMAX];
  char buffer[LINEMAX];
  FILE* pipe;

  // Formatting for ``getconf`` L1/L2/L3 size and line size.
  char format[] = "getconf -a | grep 'CACHE' | grep 'L%d' | "
             "grep -e '%sSIZE' | grep -o -e '[0-9]\\+$'";

  // Check for highest available cache level's size and line size.
  for(uint32_t l = 3; l >= 1; l--) {
    /* Obtain cache size. */
    snprintf(cmdline, LINEMAX, format, l, "[^LINE]");
    pipe = popen(cmdline, "r");

    // If failed to obtain information, attempt higher level (smaller ``l``).
    if(!pipe || !fgets(buffer, LINEMAX, pipe)) continue;

    // Set LLC level.
    SysInfo.llc_level = l;

    // Read value and save it.
    sscanf(buffer, "%ld", &( SysInfo.llc_size ));
    pclose(pipe);

    /* Obtain cache line size. */
    snprintf(cmdline, LINEMAX, format, l, "LINE");
    pipe = popen(cmdline, "r");

    // If failed to obtain information, attempt higher level (smaller ``l``).
    if(!pipe || !fgets(buffer, LINEMAX, pipe)) continue;

    // Read value and save it.
    sscanf(buffer, "%ld", &( SysInfo.line_size ));
    pclose(pipe);

    // Stop the search.
    break;
  }

  return;
}



/* Sets SysInfo.num_*, SysInfo.cpus_per_core and SysInfo.cores_per_llc and
 * populates SysInfo.LLCs (and its child arrays).
 * On Failure to properly obtain/populate this info, returns false.
 * Requires that SysInfo.llc_level/size are already filled (and are non-zero).
 */
bool prepare_sys_hierarchy() {
  uint32_t num_llcs = 0, num_cores = 0, num_cpus = 0;
  char buffer[LINEMAX];
  FILE* pipe;

  // Initialize SysInfo.cpus_per_core and SysInfo.cores_per_llc.
  SysInfo.cpus_per_core = UINT_MAX;
  SysInfo.cores_per_llc = UINT_MAX;

  /* Prepare ``lscpu`` extraction format into ``format``. */
  char format[LINEMAX];
  bool format_complete = false;
  strcpy(format, "%d,%d,");

  pipe = popen("lscpu -b -p=cache | grep -o -e 'L1.*'", "r");
  if(!pipe || !fgets(buffer, LINEMAX, pipe)) return false;

  /* Format <==> "%d, %d, (%*d:)* %d" [CPU, Core, ..., LLC]. */
  for(uint32_t i = 0; buffer[i]; i++) {
    // Ignore all caches higher than LLC.
    if(buffer[i] != ':') continue;
    strcat(format, "%*d:");

    // Terminate loop on ``Lx`` where x == llc_level.
    if(buffer[++i] == 'L' && buffer[++i] == 0x30+SysInfo.llc_level /*ASCII*/) {
      strcat(format, "%d");
      format_complete = true;
      break;
    }
  }

  pclose(pipe);

  // Unless format was properly prepared, return false.
  if(!format_complete) return false;

  /* Use ``lscpu`` to obtain CPU ID, Core ID, LLC ID per available CPU. */
  pipe = popen("lscpu -p=cpu,core,cache | grep -v '^#'", "r");
  if(!pipe) return false;

  /* Allocate array for available CPUs (to be re-sized if required). */
  uint32_t CPUs_max = 32; // Initial size (arbitrary).
  cpu_t *CPUs = malloc(CPUs_max * sizeof(cpu_t));

  while(fgets(buffer, LINEMAX, pipe)) {
    // Resize CPUs array if necessary.
    if(num_cpus >= CPUs_max) {
      CPUs_max <<= 1;
      CPUs = realloc(CPUs, CPUs_max * sizeof(cpu_t));
    }

    // Extract CPU information.
    cpu_t *cpu = CPUs + (num_cpus++);
    sscanf(buffer, format, &(cpu->id), &(cpu->core), &(cpu->llc));

    // (Re)set the number of cores/LLCs as appropriate.
    num_cores = MAX(num_cores, cpu->core+1);
    num_llcs  = MAX(num_llcs,  cpu->llc+1);
  }

  // Cleanup.
  pclose(pipe);

  /* Allocate arrays for Cores and LLCs. */
  core_t *Cores = calloc(num_cores, sizeof(core_t));
  llc_t  *LLCs  = calloc(num_llcs,  sizeof(llc_t));

  // Set the number of CPUs (as well as the parent LLC) for each core.
  for(uint32_t i = 0; i < num_cpus; i++) {
    Cores[CPUs[i].core].num_cpus++;
    Cores[CPUs[i].core].llc = CPUs[i].llc;
  }

  // Prepare the data in Cores, set number of cores for each LLC,
  // and set SysInfo.cpus_per_core.
  for(uint32_t i = 0; i < num_cores; i++) {
    core_t *core = Cores + i;

    // ASSUMPTION: Kernel wouldn't place a core ID with no usable CPUs.
    // In otherwords, the IDs must be sequential.
    assert(core->num_cpus > 0);

    // (Re)set the number of CPUs per core as appropriate.
    SysInfo.cpus_per_core = MIN(SysInfo.cpus_per_core, core->num_cpus);

    core->id       = i;
    core->CPUs     = calloc(core->num_cpus, sizeof(cpu_t*));
    core->num_cpus = 0; // Reset this to ``0`` (to be used as counter below).

    // Increment the number of cores for parent LLc.
    LLCs[core->llc].num_cores++;
  }


  // Prepare the data in LLCs and set SysInfo.cores_per_llc.
  for(uint32_t i = 0; i < num_llcs; i++) {
    llc_t *llc = LLCs + i;

    // ASSUMPTION: Kernel wouldn't place an LLC ID with no usable cores.
    // In otherwords, the IDs must be sequential.
    assert(llc->num_cores > 0);

    // (Re)set the number of cores per LLC as appropriate.
    SysInfo.cores_per_llc = MIN(SysInfo.cores_per_llc, llc->num_cores);

    llc->id    = i;
    llc->Cores = calloc(llc->num_cores, sizeof(core_t*));
    llc->num_cores = 0; // Reset this to ``0`` (to be used as counter below).
  }


  // Append each CPU to its parent core's list of CPUs.
  for(uint32_t i = 0; i < num_cpus; i++) {
    cpu_t  *cpu  = CPUs + i;
    core_t *core = Cores + cpu->core;
    core->CPUs[core->num_cpus++] = cpu;
  }

  // Append each core to its parent LLC's list of cores.
  for(uint32_t i = 0; i < num_cores; i++) {
    core_t *core = Cores + i;
    llc_t  *llc  = LLCs + core->llc;
    llc->Cores[llc->num_cores++] = core;
  }

  // Update SysInfo appropriately.
  SysInfo.LLCs  = LLCs;
  SysInfo.Cores = Cores;
  SysInfo.CPUs  = CPUs;
  SysInfo.num_llcs  = num_llcs;
  SysInfo.num_cores = num_cores;
  SysInfo.num_cpus  = num_cpus;

  return true;
}
