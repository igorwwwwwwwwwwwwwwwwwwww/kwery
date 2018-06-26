require 'set'

# this is the query planner, sometimes also called "optimizer"

module Kwery
  class Planner
    def initialize(schema, query)
      @schema = schema
      @query = query
    end

    def call
      plan = index_only_scan || agg_scan || index_scan || table_scan || empty_scan
      plan = explain(plan) if @query.options[:explain]
      plan
    end

    private

    def index_only_scan
      return unless select_agg.size == 1
      return unless @query.select.size == 1
      return unless @query.from
      return unless @query.group_by.size == 0

      # select count(*)
      k, _ = @query.select.find do |k, v|
        v == Kwery::Expr::FnCall.new(:count, [Kwery::Expr::Column.new(:*)])
      end

      return unless k

      candidates = IndexMatcher.new(@schema, @query).match
      candidate = candidates.reject { |c| c.sorted || c.recheck }.first

      return unless candidate

      plan = Kwery::Executor::IndexOnlyScan.new(
        candidate.index_name,
        candidate.sargs,
        :asc,
        @query.options,
      )

      plan = Kwery::Executor::Aggregate.new(
        k,
        Kwery::Executor::AggregateIndexOnlyScanCount.new,
        plan
      )

      plan
    end

    def agg_scan
      return unless select_agg.size > 0
      return unless @query.from

      # TODO: index-only scan

      candidates = IndexMatcher.new(@schema, @query).match
      candidate = candidates.reject { |c| c.sorted }.first

      if candidate
        plan = Kwery::Executor::IndexScan.new(
          @query.from,
          candidate.index_name,
          candidate.sargs,
          :asc,
          @query.options,
        )
        plan = where(plan) if candidate.recheck
      else
        plan = Kwery::Executor::TableScan.new(
          @query.from,
          @query.options,
        )
        plan = where(plan)
      end

      if @query.group_by.size > 0
        plan = group_by(plan)
        plan = sort(plan)
        plan = limit(plan)
      else
        # single tup result, no sort or limit needed
        plan = aggregate(plan)
      end
      plan
    end

    def index_scan
      # pick first candidate for now
      # we can do cost-based planning later
      candidates = IndexMatcher.new(@schema, @query).match
      candidate = candidates.first

      return unless candidate

      plan = Kwery::Executor::IndexScan.new(
        @query.from,
        candidate.index_name,
        candidate.sargs,
        :asc,
        @query.options,
      )

      plan = where(plan)  if candidate.recheck
      plan = sort(plan)   unless candidate.sorted
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    # cut my plans into pieces
    # this is my last resort
    def table_scan
      return unless @query.from

      plan = Kwery::Executor::TableScan.new(
        @query.from,
        @query.options,
      )

      plan = where(plan)
      plan = sort(plan)
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    def empty_scan
      plan = Kwery::Executor::EmptyScan.new

      # at most one tuple, no sort or limit needed
      plan = where(plan)
      plan = project(plan)
      plan
    end

    # where clause is an array
    # of (implicitly) ANDed expressions
    def where(plan)
      return plan unless @query.where.size > 0

      Kwery::Executor::Filter.new(
        lambda { |tup|
          @query.where.map { |cond| cond.call(tup) }.reduce(:&)
        },
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

    def project(plan)
      return plan if select_agg.size > 0

      if @query.select[:*]
        plan = Kwery::Executor::Project.new(
          lambda { |tup| tup.merge(@query.select.map { |k, f| [k, f.call(tup)] }.to_h) },
          plan
        )
      else
        plan = Kwery::Executor::Project.new(
          lambda { |tup| @query.select.map { |k, f| [k, f.call(tup)] }.to_h },
          plan
        )
      end

      plan
    end

    def group_by(plan)
      # TODO: support more than one aggregation
      k, agg = select_agg.first

      group_by = lambda { |tup|
        @query.group_by.map { |expr| expr.call(tup) }
      }

      group_keys = @query.group_by.map do |g|
        @query.select
          .find { |k,v| v == g }
          &.first # key of pair
      end

      Kwery::Executor::HashAggregate.new(
        k,
        agg,
        group_by,
        group_keys,
        plan
      )
    end

    def aggregate(plan)
      # TODO: support more than one aggregation
      k, agg = select_agg.first
      Kwery::Executor::Aggregate.new(
        k,
        agg,
        plan
      )
    end

    def explain(plan)
      plan = Kwery::Executor::Explain.new(plan)
      plan
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
