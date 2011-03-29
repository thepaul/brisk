include "cassandra.thrift"

namespace java org.apache.cassandra.thrift
namespace cpp org.apache.cassandra
namespace csharp Apache.Cassandra
namespace py cassandra
namespace php cassandra
namespace perl Cassandra
namespace rb CassandraThrift

service Brisk extends cassandra.Cassandra
{
  /**  returns (in order) the endpoints for each key specified. */
  list<list<string>> describe_keys(1:required string keyspace, 2:required list<binary> keys);
}