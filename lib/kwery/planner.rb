require 'set'

# this is the query planner, sometimes also called "optimizer"

module Kwery
  class Planner
    def initialize(catalog, query)
      @catalog = catalog
      @query = query
    end

    def call
      index_scan || table_scan
    end

    private

    def index_scan
      matcher = IndexMatcher.new(@catalog, @query)

      candidates = matcher.match

      return if candidates.empty?

      # pick first candidate for now
      # we can do cost-based planning later
      candidate = candidates.first

      plan = Kwery::Executor::IndexScan.new(
        @query.from,
        candidate.index_name,
        candidate.sargs,
        :asc,
      )

      plan = limit(plan)
      plan = project(plan)
      plan
    end

    def match_prefix(index_exprs, match_exprs)
      (1..index_exprs.size)
        .map {|i| index_exprs.each_slice(i).to_a }
        .reverse
        .select { |prefix, remainder| prefix.to_set == match_exprs.to_set }
        .map { |prefix, remainder| prefix }
        .first
    end

    # cut my plans into pieces
    # this is my last resort
    def table_scan
      if @query.options[:notablescan]
        # a notable scan indeed
        raise Kwery::Executor::NoTableScanError.new("query resulted in table scan")
      end

      plan = Kwery::Executor::TableScan.new(@query.from)

      plan = where(plan)
      plan = sort(plan)
      plan = limit(plan)
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
      plan = Kwery::Executor::Project.new(
        lambda { |tup| @query.select.map { |k, f| [k, f.call(tup)] }.to_h },
        plan
      )

      plan
    end
  end
end
