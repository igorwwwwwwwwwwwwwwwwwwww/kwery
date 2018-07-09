require 'set'

# TODO: standalone (non-sharded) mode?
# TODO: forward RESHARD MOVE from the proxy

module Kwery
  class Planner
    class Reshard
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        reshard_move_query || reshard_receive_query
      end

      private

      def reshard_move_query
        return unless Kwery::Query::ReshardMove === @query

        plan = Kwery::Executor::ReshardMove.new

        plan
      end

      def reshard_receive_query
        return unless Kwery::Query::ReshardReceive === @query

        plan = Kwery::Executor::ReshardReceive.new

        plan
      end
    end
  end
end
