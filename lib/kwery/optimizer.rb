require 'set'

# this is the "query planner"

module Kwery
  class Optimizer
    def initialize(catalog, query)
      @catalog = catalog
      @query = query
    end

    def call
      index_scan_backward || index_scan || table_scan
    end

    private

    def index_scan_backward
      return unless @query.where.size > 0 && @query.order_by.size > 0

      index_name = @catalog.tables[@query.from].indexes
        .map { |k| [k, @catalog.indexes[k]] }
        .select { |name, idx| idx.indexed_exprs_reverse == @query.order_by }
        .map { |k, _| k }
        .first

      return unless index_name

      plan = Kwery::Executor::IndexScan.new(@query.from, index_name, {}, :desc)

      plan = where(plan)
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    def index_scan
      return unless @query.where.size > 0 || @query.order_by.size > 0

      # TODO: support using index for both WHERE and ORDER BY
      if @query.where.size > 0
        match_cols_map = @query.where
          .select { |expr| Kwery::Expr::Eq === expr }
          .select { |expr| Kwery::Expr::Column === expr.left }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left.name, expr.right.value] }
          .to_h
        match_cols = match_cols_map.keys.to_set

        index_name = @catalog.tables[@query.from].indexes
          .map { |k| [k, @catalog.indexes[k]] }
          .map { |k, idx| [k, idx.indexed_exprs.map(&:expr)] }
          .map { |k, exprs| [k, exprs.map(&:name).to_set] }
          .select { |k, indexed_cols| indexed_cols == match_cols }
          .map { |k, _| k }
          .first

        index = @catalog.indexes[index_name]
        if index
          eq_key = index.indexed_exprs
            .map(&:expr)
            .map(&:name)
            .map { |k| match_cols_map[k] }

          sargs = {eq: eq_key}

          plan = Kwery::Executor::IndexScan.new(@query.from, index_name, sargs, :asc)

          plan = limit(plan)
          plan = project(plan)
          return plan
        end
      end

      # exact match on order by
      index_name = @catalog.tables[@query.from].indexes
        .map { |k| [k, @catalog.indexes[k]] }
        .select { |name, idx| idx.indexed_exprs == @query.order_by }
        .map { |k, _| k }
        .first

      return unless index_name

      plan = Kwery::Executor::IndexScan.new(@query.from, index_name, {}, :asc)

      # TODO: extra where on index prefix match
      # TODO: extra sort on index prefix match
      plan = limit(plan)
      plan = project(plan)
      plan
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

    # TODO: change this to be DNF (OR of ANDs)
    #
    # therewhere clause is an array
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
