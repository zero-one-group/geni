(ns zero-one.geni.core.storage
  (:import
    (org.apache.spark.storage StorageLevel)))

(def disk-only (StorageLevel/DISK_ONLY))

(def disk-only-2 (StorageLevel/DISK_ONLY_2))

(def memory-and-disk (StorageLevel/MEMORY_AND_DISK))

(def memory-and-disk-2 (StorageLevel/MEMORY_AND_DISK_2))

(def memory-and-disk-ser (StorageLevel/MEMORY_AND_DISK_SER))

(def memory-and-disk-ser-2 (StorageLevel/MEMORY_AND_DISK_SER_2))

(def memory-only (StorageLevel/MEMORY_ONLY))

(def memory-only-2 (StorageLevel/MEMORY_ONLY_2))

(def memory-only-ser (StorageLevel/MEMORY_ONLY_SER))

(def memory-only-ser-2 (StorageLevel/MEMORY_ONLY_SER_2))

(def none (StorageLevel/NONE))

(def off-heap (StorageLevel/OFF_HEAP))
