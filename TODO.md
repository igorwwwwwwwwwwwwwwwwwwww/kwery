# vague ideas

* query engine
  * table scan
  * in-memory sort
  * index scan with offset
  * collect runtime statistics/traces
* query planner
  * index conditions from where clause
  * exact matching on multi-column indexes
  * prefix matching (pick longest prefix)
  * re-check on imperfect index
* sql parser

# vague future

* partial indexes
* insert/update/delete
* write-ahead log (maybe use kafka)
* replication
* sharding (requires client&protocol)
