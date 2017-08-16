WaterMarkedBlockingQueue
======

A wrapper for blocking queue that takes an additional parameter of lowWaterMark. Once the queue goes fill until
the number of elements in the queue is same as lowWaterMark, the queue is treated as full, for all further inserts.

