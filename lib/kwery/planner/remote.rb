require 'set'

module Kwery
  class Planner
    class Remote
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        return unless @query.options[:remote]
        select_query
      end

      private

      def select_query
        shards = match_shards

        backends = @schema.backends_for_shards(@query.from, shards)
        backends = @schema.backends_all(@query.from) if backends.size == 0

        plans = backends.map do |backend, shards|
          Kwery::Executor::Remote.new(
            shards,
            backend,
            @query.options[:sql],
          )
        end

        plan = Kwery::Executor::Append.new(plans)
        plan = combine_aggregates(plan)
        plan = sort(plan)
        plan = limit(plan)

        plan
      end

      def combine_aggregates(plan)
        return plan unless select_agg.size > 0
        return plan unless @query.from

        k, agg = select_agg.first

        Kwery::Executor::CombineAggregates.new(
          k,
          agg,
          plan
        )
      end

      def limit(plan)
        return plan unless @query.limit

        Kwery::Executor::Limit.new(@query.limit, plan)
      end

      def sort(plan)
        return plan unless @query.order_by.size > 0

        Kwery::Executor::Sort.new(
          lambda { |tup_a, tup_b|
            # => enum of ordered_col fields
            # => enum of ruby "spaceship" results (-1|0|1)
            # => take first non value that is not 0 (tup_a != tup_b)
            # => fall back to 0 if none found
            @query.order_by
              .lazy
              .map { |ordered_col|
                a = ordered_col.expr.call(tup_a)
                b = ordered_col.expr.call(tup_b)
                if ordered_col.order == :asc
                  a <=> b
                else
                  b <=> a
                end
              }
              .reject { |res| res == 0 }
              .first || 0
          },
          plan
        )
      end

      def match_shards
        shard_config = @schema.shard(@query.from)
        shard_key = shard_config[:key]

        @query.where
          .select { |expr| Kwery::Expr::Eq === expr }
          .select { |expr| expr.left == shard_key }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| expr.right.value }
          .map { |val| @schema.shard_for_value(@query.from, val) }
          .uniq
      end

      def select_agg
        @select_agg ||= @query.select
          .select { |k,v|
            Kwery::Expr::FnCall === v && Kwery::Expr::AGG_FN_TABLE[v.fn_name]
          }
          .map { |k,v| [k, Kwery::Expr::AGG_FN_TABLE[v.fn_name].call(v.exprs)] }
          .to_h
      end
    end
  end
end
