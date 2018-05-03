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
      index_scan(schema) || table_scan(schema)
    end

    private

    def index_scan(schema)
      unless @order_by.size == 1
        return
      end

      # TODO: remove matching prefix, if the remainder of the index spec
      #       is an exact inverse of the order_by spec, we can perform a
      #       backward-scan.

      index_name = schema
        .indexes
        .values
        .select { |idx| idx.columns == @order_by }
        .map { |idx| idx.name }
        .first
      unless index_name
        return
      end

      plan = Kwery::Plan::IndexScan.new(@from, index_name, @order_by.first.order)

      plan = where(plan)
      # TODO: extra sort on partial index match
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    # cut my plans into pieces
    # this is my last resort
    def table_scan(schema)
      plan = Kwery::Plan::TableScan.new(@from)

      plan = where(plan)
      plan = sort(plan)
      plan = limit(plan)
      plan = project(plan)
      plan
    end

    # TODO: make this optional if full index match
    def where(plan)
      if @where
        plan = Kwery::Plan::Filter.new(
          lambda { |tup| @where.call(tup) },
          plan
        )
      end

      plan
    end

    def limit(plan)
      if @limit
        plan = Kwery::Plan::Limit.new(@limit, plan)
      end

      plan
    end

    def sort(plan)
      if @order_by.size > 0
        plan = Kwery::Plan::Sort.new(
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

      plan
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

    class OrderedField < Struct.new(:expr, :order)
    end
  end
end
