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
        single_backend_select_query || select_query || insert_query || update_query || delete_query || unsupported_query
      end

      private

      def single_backend_select_query
        return unless Kwery::Query::Select === @query

        shards = match_shards(@query.from, @query.where)

        backends = @schema.backends_for_shards(@query.from, shards)
        backends = @schema.backends_all(@query.from) if backends.size == 0

        return unless backends.size == 1

        # no partial aggregation, no combine_aggregates
        # no re-sorting, no re-limiting needed
        # essentially a pass-through / forward to backend

        plan = Kwery::Executor::RemoteBatch.new(
          backends,
          @query.options[:sql],
        )

        plan
      end

      def select_query
        return unless Kwery::Query::Select === @query

        shards = match_shards(@query.from, @query.where)

        backends = @schema.backends_for_shards(@query.from, shards)
        backends = @schema.backends_all(@query.from) if backends.size == 0

        plan = Kwery::Executor::RemoteBatch.new(
          backends,
          @query.options[:sql],
          partial: true,
        )

        plan = combine_aggregates(plan)
        plan = sort(plan)
        plan = limit(plan)

        plan
      end

      def insert_query
        return unless Kwery::Query::Insert === @query

        # TODO: delay evaluation of expressions until runtime

        tups = @query.values.map do |row|
          Hash[@query.keys.zip(row.map { |expr| expr.call({}) })]
        end

        backends = tups.group_by do |tup|
          shard = @schema.shard_for_tup(@query.into, tup)
          @schema.primary_for_shard(@query.into, shard)
        end

        # TODO: replace RemoteInsert with RemoteBatch
        #       this requires producing a filtered insert sql
        #       for each backend.

        plans = backends.map do |backend, tups|
          Kwery::Executor::RemoteInsert.new(
            backend,
            @query.into,
            tups,
          )
        end

        plan = Kwery::Executor::Append.new(plans)
        plan = Kwery::Executor::MergeCounts.new(plan)

        plan
      end

      def update_query
        return unless Kwery::Query::Update === @query
      end

      def delete_query
        return unless Kwery::Query::Delete === @query
      end

      def unsupported_query
        raise Kwery::Planner::UnsupportedQueryError.new(
          'query is not supported by proxy'
        )
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

      def match_shards(from, where)
        shard_config = @schema.shard(from)
        shard_key = shard_config[:key]

        vals = []

        vals.concat where
          .select { |expr| Kwery::Expr::Eq === expr }
          .select { |expr| expr.left == shard_key }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| expr.right.value }

        vals.concat where
          .select { |expr| Kwery::Expr::In === expr }
          .select { |expr| expr.expr == shard_key }
          .select { |expr| expr.vals.all? { |val| Kwery::Expr::Literal === val } }
          .flat_map { |expr| expr.vals.map(&:value) }

        vals
          .map { |val| @schema.shard_for_value(from, val) }
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
