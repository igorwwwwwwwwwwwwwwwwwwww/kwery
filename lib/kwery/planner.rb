require 'set'

# this is the query planner, sometimes also called "optimizer"

module Kwery
  class Planner
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
        match_exprs_map = @query.where
          .select { |expr| Kwery::Expr::Eq === expr }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left, expr.right.value] }
          .to_h
        match_exprs = match_exprs_map.keys.to_set

        range_exprs_map = @query.where
          .select { |expr| Kwery::Expr::Gt === expr }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left, expr.right.value] }
          .to_h
        range_exprs = range_exprs_map.keys.to_set

        index_exprs = @catalog.tables[@query.from].indexes
          .map { |k| [k, @catalog.indexes[k]] }
          .map { |k, idx| [k, idx.indexed_exprs.map(&:expr)] }

        # TODO: support multiple range expressions on the same column
        #       e.g. id > 10 AND id < 20
        #
        # try exact range match
        if match_exprs.size == 0 && range_exprs.size == 1
          index_name = index_exprs
            .select { |k, exprs| exprs.to_set == range_exprs }
            .map { |k, exprs| k }
            .first

          if index_name
            index = @catalog.indexes[index_name]

            gt_key = index.indexed_exprs
              .map(&:expr)
              .map { |k| range_exprs_map[k] }

            sargs = {gt: gt_key}

            plan = Kwery::Executor::IndexScan.new(@query.from, index_name, sargs, :asc)

            plan = limit(plan)
            plan = project(plan)
            return plan
          end
        end

        index_name, matched_prefix = index_exprs
          .map { |k, exprs| [k, match_prefix(exprs, match_exprs)] }
          .select { |k, prefix| prefix }
          .first

        if index_name
          index = @catalog.indexes[index_name]

          match_remainder = index.indexed_exprs
            .map(&:expr)
            .drop(matched_prefix.size)

          # suffix range match
          if range_exprs.size == 1 && match_remainder.to_set == range_exprs
            index = @catalog.indexes[index_name]

            gt_key = index.indexed_exprs
              .map(&:expr)
              .map { |k| match_exprs_map[k] || range_exprs_map[k] }

            sargs = {gt: gt_key}

            plan = Kwery::Executor::IndexScan.new(@query.from, index_name, sargs, :asc)

            plan = limit(plan)
            plan = project(plan)
            return plan
          end

          eq_key = index.indexed_exprs
            .map(&:expr)
            .map { |k| match_exprs_map[k] }
            .select { |k| k }

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
