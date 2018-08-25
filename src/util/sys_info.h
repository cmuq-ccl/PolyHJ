/*
 * PolyHJ: Polymorphic Hash Join.
 * SysInfo Header File: Types and Client Functions Declarations.
 * Refer to ``util/sys_info.c`` for more information.
 */

#ifndef _SYS_INFO_TYPES_
  #define _SYS_INFO_TYPES_

  #include <stdint.h> /* uint32_t */

  /* CPU Type Definition. */
  typedef struct {
    uint32_t id;   // Kernel's CPU ID; not necessarily sequential.
    uint32_t core; // Physical core (parent) of CPU.
    uint32_t llc;  // LLC ID, parent of core.
  } cpu_t;


  /* Core Type Definition. */
  typedef struct {
    uint32_t id;       // Core ID.
    uint32_t num_cpus; // Number of CPUs on physical core (hyperthreads).
    cpu_t  **CPUs;     // List of CPUs.
    uint32_t llc;      // LLC ID, parent to CPU.
  } core_t;


  /* Last-level Cache Type Definition. */
  typedef struct {
    uint32_t id;          // LLC ID.
    uint32_t num_cores;   // Number of physical cores on LLC.
    core_t  **Cores;      // List of Cores.
  } llc_t;


  /* ``SysInfo`` Type. */
  typedef struct {
    /* Hardware Stats. */
    uint8_t  llc_level; /* L1, L2 or L3 is LLC? */
    uint64_t llc_size;  /* In Bytes. */
    uint64_t line_size; /* Last-level cache line size. */
    uint64_t page_size;

    /* LLC(s) > Core(s) > CPU(s) Hierarchical Structure. */
    llc_t   *LLCs;
    uint32_t num_llcs;
    core_t  *Cores;
    uint32_t num_cores;
    cpu_t   *CPUs;
    uint32_t num_cpus;

    /* Hierarchical Structure Stats. */
    uint32_t cores_per_llc; // If variation exists, these are set to
    uint32_t cpus_per_core; // the minimum, non-zero value.
  } sys_info_t;


  /* ``SysInfo`` Client Functions Declarations. */
  void sys_info_prepare();
  void sys_info_cleanup();

#endif
