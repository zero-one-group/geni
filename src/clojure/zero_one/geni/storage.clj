(ns zero-one.geni.storage
  "Spark's storage levels, for `persist`. `memory-and-disk` is a Dataset's
  default."
  (:import
   (org.apache.spark.storage StorageLevel)))

(def disk-only
  "Stores the partitions on disk only."
  (StorageLevel/DISK_ONLY))

(def disk-only-2
  "As `disk-only`, with each partition on two nodes."
  (StorageLevel/DISK_ONLY_2))

(def memory-and-disk
  "Stores the partitions in memory, and those that don't fit on disk."
  (StorageLevel/MEMORY_AND_DISK))

(def memory-and-disk-2
  "As `memory-and-disk`, with each partition on two nodes."
  (StorageLevel/MEMORY_AND_DISK_2))

(def memory-and-disk-ser
  "As `memory-and-disk`, with the partitions in memory serialised."
  (StorageLevel/MEMORY_AND_DISK_SER))

(def memory-and-disk-ser-2
  "As `memory-and-disk-ser`, with each partition on two nodes."
  (StorageLevel/MEMORY_AND_DISK_SER_2))

(def memory-only
  "Stores the partitions in memory only, and recomputes those that don't fit."
  (StorageLevel/MEMORY_ONLY))

(def memory-only-2
  "As `memory-only`, with each partition on two nodes."
  (StorageLevel/MEMORY_ONLY_2))

(def memory-only-ser
  "As `memory-only`, with the partitions serialised, which takes less memory
  and more CPU."
  (StorageLevel/MEMORY_ONLY_SER))

(def memory-only-ser-2
  "As `memory-only-ser`, with each partition on two nodes."
  (StorageLevel/MEMORY_ONLY_SER_2))

(def none
  "No storage."
  (StorageLevel/NONE))

(def off-heap
  "Stores the partitions serialised in off-heap memory."
  (StorageLevel/OFF_HEAP))
