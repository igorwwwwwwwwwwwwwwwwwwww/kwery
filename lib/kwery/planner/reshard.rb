require 'set'

module Kwery
  class Planner
    class Reshard
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        reshard_move_query
      end

      private

      def reshard_move_query
        return unless Kwery::Query::ReshardMove === @query

        if @schema.shards.locked?(@query.table, @query.shard)
          raise Kwery::Shard::ShardLockedError.new
        end

        plan = Kwery::Executor::ReshardMove.new(
          @query.table,
          @query.shard,
          @query.target,
        )

        plan
      end
    end
  end
end
