require 'set'

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
      unless @query.order_by.size > 0
        return
      end

      index = @catalog.tables[@query.from].indexes.values
        .select { |idx| idx.reverse == @query.order_by }
        .first
      unless index
        return
      end

      plan = Kwery::Plan::IndexScan.new(@query.from, index.name, :desc)

      plan = where(plan)
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    def index_scan
      # TODO: extract index bounds from WHERE
      # TODO: support using index for both WHERE and ORDER BY
      if @query.where
        # * get all expressions being compared to constant values
        # * search all indexes to see if any of them are satisfied
        #   by our set of known constants
        # * we can now compile that into a key (range) that can be
        #   scanned by the index

        # next steps: start with the executor, then we have a target
        #   api to work against.
        # then: try and find some papers on query planning, maybe
        #   vldb has something.

        comparison_operators = Set.new([Kwery::Expr::Eq, Kwery::Expr::Gt])
        match_exprs_map = @query.where
          .select { |expr| comparison_operators.include?(expr.class) }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left, expr.right] }
          .to_h
        match_exprs = match_exprs_map.keys.to_set

        matching_indexes = @catalog.tables[@query.from].indexes
          .map { |k| [k, @catalog.indexes[k]] }
          .map { |k, index| [k, index.indexed_exprs.map(&:expr).to_set] }
          .select { |k, exprs| exprs == match_exprs }
          .to_h

        index_name = matching_indexes.keys.first

        # TODO pass this as a condition to the index scan
        index_options = match_exprs_map
      end

      unless index_name
        # try to find index with exact match
        index = @catalog.tables[@query.from].indexes
          .select { |name, idx| idx.columns == @query.order_by }
          .keys
          .first
      end

      unless index_name
        return
      end

      plan = Kwery::Plan::IndexScan.new(@query.from, index_name, :asc)

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
        raise Kwery::Plan::NoTableScanError.new("query resulted in table scan")
      end

      plan = Kwery::Plan::TableScan.new(@query.from)

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
      return plan unless @query.where

      Kwery::Plan::Filter.new(
        lambda { |tup|
          @query.where.map { |cond| cond.call(tup) }.reduce(:&)
        },
        plan
      )
    end

    def limit(plan)
      return plan unless @query.limit

      Kwery::Plan::Limit.new(@query.limit, plan)
    end

    def sort(plan)
      return plan unless @query.order_by.size > 0

      Kwery::Plan::Sort.new(
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
      plan = Kwery::Plan::Project.new(
        lambda { |tup| @query.select.map { |k, f| [k, f.call(tup)] }.to_h },
        plan
      )

      plan
    end
  end
end
