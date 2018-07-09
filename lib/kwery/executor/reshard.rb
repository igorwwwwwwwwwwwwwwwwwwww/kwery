# resharding
#
# a lot of the enable/disable reads comes from the lack
# of transactions. if we could make the reshard transactional
# we could wait with the commit.
#
# alternatively, having one table-per-shard would also make it
# a lot easier to toggle visibility. that would move some
# complexity to the query engine, since it will need to perform
# more fan-out (parallel append, similar to batch client but
# in-process).
#
# this would also allow for one thread per partition, allowing
# for horizontal scaling across cores. however, scalability could
# also be accomplished by running multiple backends on a single
# machine (listening on different ports), which given ruby's lack
# of true multi-threading might be the better option.

# order of actions
# * source: reject writes
# * target: filter reads (all reads now go through consensus)
# * source: copy to target
# * source: filter reads (consensus)
# * target: unfilter reads (consensus)
# * target: accept writes
# * source: delete data
# * source: unfilter reads

# state machine (source)
# * default
# v   RESHARD <table> MOVE <shard> TO <target-backend> (from user)
# * reshard-copying-init
# v   reject writes
# * reshard-copying-wait
# v   background: copy to target (send RESHARD RECEIVE)
# * reshard-cutover-init
# v   reject reads
# * reshard-cutover-wait
# v   background: consensus await shard owner (barrier)
# * reshard-cutover-commit
# v   accept reads
# v   filter reads
# * reshard-cleanup
# v   background: delete data
# v   unfilter reads
# * default              (resharding complete)

# state machine (target)
# * default
# v   RESHARD <table> RECEIVE <shard> (from source)
# * reshard-copying-init
# v   filter reads
# * reshard-copying-wait
# v   background: receive data
# * reshard-cutover-init
# v   reject reads
# * reshard-cutover-wait
# v   background: consensus write shard owner (barrier)
# * reshard-cutover-commit
# v   accept reads
# v   unfilter reads
# v   accept writes
# * default              (resharding complete)

module Kwery
  module Executor
    class ReshardMove
      def initialize(table, shard, target)
        @table  = table
        @shard  = shard
        @target = target
      end

      def call(context)
        # * default
        # v   RESHARD <table> MOVE <shard> TO <target-backend> (from user)

        # * reshard-copying-from
        # v   reject writes
        # TODO: once writes are disabled, every subsequent read needs to go
        #       through consensus, so that we can ensure we still own the
        #       shard.
        @context.schema.reject_writes(@table, @shard)

        # v   copy to target (send RESHARD RECEIVE)
        # TODO: read via sub plans?
        client = @context.client
        client.reshard_copy(@target, @table, @shard)

        # v   consensus await shard owner (barrier)
        # TODO: do not service any requests during this operation
        # TODO: timeout
        consensus = @context.consensus
        consensus.await("owner:#{@table}:#{@shard}", @target)

        # * reshard-cutover-from
        # v   filter reads
        @context.schema.filter_reads(@table, @shard)

        # v   delete data
        # TODO: delete via sub plans?

        # v   unfilter reads
        @context.schema.unfilter_reads(@table, @shard)

        # * default              (resharding complete)

        [{ success: true }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class ReshardReceive
      def initialize(table, shard, format)
        @table = table
        @shard = shard
        @format = format
      end

      def call(context)
        # * default
        # v   RESHARD <table> RECEIVE <shard> (from source)

        # * reshard-copying-to
        # v   filter reads
        @context.schema.filter_reads(@table, @shard)

        # v   receive data
        tups = @format.load(context.stdin, {}, context)
        context.schema.bulk_insert(@table, tups)

        # v   consensus write shard owner (barrier)
        consensus = @context.consensus
        consensus.set("owner:#{@table}:#{@shard}", @context.backend)

        # * reshard-cutover-to
        # v   unfilter reads
        # v   accept writes
        @context.schema.unfilter_reads(@table, @shard)
        @context.schema.accept_writes(@table, @shard)

        # * default              (resharding complete)

        [{ success: true }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end
  end
end
