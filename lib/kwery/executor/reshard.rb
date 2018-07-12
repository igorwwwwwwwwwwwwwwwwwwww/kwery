# resharding

# order of actions (simplified edition)
# * source: lock shard (reject reads and writes)
# * target: lock shard
# * source: copy to target
# * source: delete data
# * source: unlock shard
# * target: unlock shard

# TODO: move resharding to separate process, process async via queue
# TODO: use consensus to establish shard assignments across proxies
# TODO: online resharding without excessive locking
# TODO: incremental data copy
# TODO: log shard reassignment to journal?

module Kwery
  module Executor
    class ReshardMove
      # target is the rs (replica set) number
      def initialize(table, shard, target)
        @table  = table
        @shard  = shard
        @target = target
      end

      def call(context)
        # TODO: check locks when executing other queries
        context.shards.lock(@table, @shard)

        sharding_key = context.shards.key(@table)
        num_shards   = context.shards.count(@table)
        source       = context.shards.primary_for_shard(@table, @shard)

        client = Kwery::Client::Batch.new

        result = client.query(
          source => "SELECT * FROM #{@table} WHERE hashmod(#{sharding_key}, #{num_shards}) = #{@shard}",
        )
        tups = result.first[:data]

        format = Kwery::Format::Json.new
        stdin = format.encode(tups).join

        # TODO: split large stream into smaller requests
        target = context.shards.primary_for_rs(@target)
        client.query(
          target => {
            query: "COPY #{@table} FROM STDIN",
            data:  stdin,
          },
        )

        client.query(
          source => "DELETE FROM #{@table} WHERE hashmod(#{sharding_key}, #{num_shards}) = #{@shard}",
        )

        context.shards.reassign(@table, @shard, @target)
        context.shards.unlock(@table, @shard)

        [{ success: true }]
      end

      def explain(context)
        [self.class, @table, @shard, @target]
      end
    end
  end
end
