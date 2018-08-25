/*
 * PolyHJ: Polymorphic Hash Join.
 * > Relation-related Types:
 *    # tuple_t: (tkey_t, tpayload_t)
 *    # bucket_t
 *    # relation_t
 *
 * > Threads Meta-Data Type:
 *    # thread_t
 *
 * > Global Parameters Types:
 *    # radix_info_t
 *    # params_t
 */


#ifndef __PolyHJ_TYPES_H__
  #define __PolyHJ_TYPES_H__

  /* Tuples (as well as keys, payloads and buckets). */
  typedef uint32_t   tkey_t;
  typedef uint32_t   tpayload_t;
  typedef tpayload_t bucket_t;
  typedef struct { tkey_t key; tpayload_t payload; } tuple_t;


  /* Relations (and Sub-Relations). */
  typedef struct {
    tuple_t *tuples;
    uint32_t size;   // Number of tuples.
    uint32_t offset; // within parent relation (for sub-relations).
    uint32_t seed;
    double   skew;
    char     id;     // Relation 'R' or 'S'?
  } relation_t;


  /* Block Data for ICP. */
  typedef struct { uint32_t start, end; } block_t;
  typedef struct { uint32_t  N; block_t **Pos; } block_meta_t;


  /* Thread Meta-data Type. */
  typedef struct {
    /* IDs. */
    uint32_t     tid;   // Thread ID.
    uint32_t     group; // Group ID for ColBP (0 <= group < Threads.num_groups)

    /* Sub-Relations. */
    relation_t  *SubR;
    relation_t  *SubS;

    /* ICP Data. */
    block_meta_t BlocksR;
    block_meta_t BlocksS;

    /* Join Stats (for thread's sub-relations). */
    uint64_t     matches;
    uint64_t     checksum;

    /* CPU-related Information. */
    cpu_t       *CPU;   // Info about thread's assigned CPU.
  } thread_t;


  /* Fanout-related Parameters. */
  typedef struct {
    uint32_t R; // # of radix bits for partitioning relation R.
    uint32_t S; // # of radix bits for partitioning relation S.
    bool     user_defined; // true iff user has supplied radices.
  } radix_info_t;


  /* Global Parameters. */
  typedef struct {
    /* Based on Input Parameters. */
    uint32_t    N;       // Number of threads running the join.
    relation_t *RelR;    // Relation R.
    relation_t *RelS;    // Relation S.
    bucket_t  **HTables; // Shared Hash Table(s).
    bool        favor_physical_cores;

    /* Populated by prepare_threads_meta(). */
    thread_t   *Args;          // Threads Arguments.
    uint32_t    num_groups;    // # of groups for ColBP
    uint32_t    utilized_llcs; // num_groups == utilized_llcs
    uint32_t    utilized_cpus_per_core;
  } params_t;


  /* Timer. */
  typedef struct timespec timespec;
  typedef struct { timespec checkpoint; double elapsed; } ttimer_t;

  /* Random Number Generation. */
  typedef struct { uint32_t w, x, y, z; } randgen_t;


#endif
