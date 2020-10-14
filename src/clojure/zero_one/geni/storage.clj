; Docstring Sources:
; https://sparkbyexamples.com/spark/spark-persistence-storage-levels
; https://www.pgs-soft.com/blog/spark-memory-management-part-1-push-it-to-the-limits/
; https://stackoverflow.com/questions/51051888/spark-storage-level-none-vs-memory-only
(ns zero-one.geni.storage
  (:import
    (org.apache.spark.storage StorageLevel)))

(def disk-only
  "Flag for controlling the storage of an RDD.

  DataFrame is stored only on disk and the CPU computation time is high as I/O involved."
  (StorageLevel/DISK_ONLY))

(def disk-only-2
  "Flag for controlling the storage of an RDD.

  Same as disk-only storage level but replicate each partition to two cluster nodes."
  (StorageLevel/DISK_ONLY_2))

(def memory-and-disk
  "Flag for controlling the storage of an RDD.

  The default behavior of the DataFrame or Dataset. In this Storage Level, The DataFrame will be stored in JVM memory as deserialized objects. When required storage is greater than available memory, it stores some of the excess partitions into a disk and reads the data from disk when it required. It is slower as there is I/O involved."
  (StorageLevel/MEMORY_AND_DISK))

(def memory-and-disk-2
  "Flag for controlling the storage of an RDD.

  Same as memory-and-disk storage level but replicate each partition to two cluster nodes."
  (StorageLevel/MEMORY_AND_DISK_2))

(def memory-and-disk-ser
  "Flag for controlling the storage of an RDD.

  Same as `memory-and-disk` storage level difference being it serializes the DataFrame objects in memory and on disk when space not available."
  (StorageLevel/MEMORY_AND_DISK_SER))

(def memory-and-disk-ser-2
  "Flag for controlling the storage of an RDD.

  Same as memory-and-disk-ser storage level but replicate each partition to two cluster nodes."
  (StorageLevel/MEMORY_AND_DISK_SER_2))

(def memory-only
  "Flag for controlling the storage of an RDD."
  (StorageLevel/MEMORY_ONLY))

(def memory-only-2
  "Flag for controlling the storage of an RDD.

  Same as `memory-only` storage level but replicate each partition to two cluster nodes."
  (StorageLevel/MEMORY_ONLY_2))

(def memory-only-ser
  "Flag for controlling the storage of an RDD.

  Same as `memory-only` but the difference being it stores RDD as serialized objects to JVM memory. It takes lesser memory (space-efficient) then `memory-only` as it saves objects as serialized and takes an additional few more CPU cycles in order to deserialize."
  (StorageLevel/MEMORY_ONLY_SER))

(def memory-only-ser-2
  "Flag for controlling the storage of an RDD.

  Same as `memory-only-ser` storage level but replicate each partition to two cluster nodes."
  (StorageLevel/MEMORY_ONLY_SER_2))

(def none
  "Flag for controlling the storage of an RDD.

  No caching."
  (StorageLevel/NONE))

(def off-heap
  "Flag for controlling the storage of an RDD.

  Off-heap refers to objects (serialised to byte array) that are managed by the operating system but stored outside the process heap in native memory (therefore, they are not processed by the garbage collector). Accessing this data is slightly slower than accessing the on-heap storage but still faster than reading/writing from a disk. The downside is that the user has to manually deal with managing the allocated memory."
  (StorageLevel/OFF_HEAP))
