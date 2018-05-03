require 'set'

module Kwery
  class Query
    def initialize(select:, from:, where: nil, order: [], limit: nil)
      @select = select
      @from = from
      @where = where
      @order_by = order
      @limit = limit
    end

    def plan(schema)
      index_scan_backward(schema) || index_scan(schema) || table_scan(schema)
    end

    private

    def index_scan_backward(schema)
      unless @order_by.size > 0
        return
      end

      index = schema.indexes.values
        .select { |idx| idx.columns_flipped == @order_by }
        .first
      unless index
        return
      end

      plan = Kwery::Plan::IndexScan.new(@from, index.name, :desc)

      plan = where(plan)
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    def index_scan(schema)
      unless @order_by.size > 0
        return
      end

      index = schema.indexes.values
        .select { |idx| idx.columns == @order_by }
        .first
      unless index
        return
      end

      # TODO: extract index bounds from WHERE
      if @where
        comparison_operators = Set.new(Kwery::Query::Eq, Kwery::Query::Gt)
        @where.select { |expr| comparison_operators.include?(expr) }
      end

      plan = Kwery::Plan::IndexScan.new(@from, index.name, :asc)

      # TODO: extra where on index prefix match
      # TODO: extra sort on index prefix match
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    # cut my plans into pieces
    # this is my last resort
    def table_scan(schema)
      if ENV['NOTABLESCAN'] == 'true'
        # a notable scan indeed
        raise "query resulted in table scan"
      end

      plan = Kwery::Plan::TableScan.new(@from)

      plan = where(plan)
      plan = sort(plan)
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    # there where clause is an array
    # of (implicitly) ANDed expressions
    def where(plan)
      return plan unless @where

      Kwery::Plan::Filter.new(
        lambda { |tup|
          @where.map { |cond| cond.call(tup) }.reduce(:&)
        },
        plan
      )
    end

    def limit(plan)
      return plan unless @limit

      Kwery::Plan::Limit.new(@limit, plan)
    end

    def sort(plan)
      return plan unless @order_by.size > 0

      Kwery::Plan::Sort.new(
        lambda { |tup_a, tup_b|
          # => enum of ordered_col fields
          # => enum of ruby "spaceship" results (-1|0|1)
          # => take first non value that is not 0 (tup_a != tup_b)
          # => fall back to 0 if none found
          @order_by
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
        lambda { |tup| @select.map { |k, f| [k, f.call(tup)] }.to_h },
        plan
      )

      plan
    end

    class Field < Struct.new(:table, :column)
      def call(tup)
        tup[column]
      end
    end

    class Literal < Struct.new(:value)
      def call(tup)
        value
      end
    end

    class Eq < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) == right.call(tup)
      end
    end

    class Gt < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) > right.call(tup)
      end
    end

    class OrderedField < Struct.new(:expr, :order)
    end
  end
end
