module Kwery
  class Query
    def initialize(select:, from:, where: nil, order: [], limit: nil)
      @select = select
      @from = from
      @where = where
      @order = order
      @limit = limit
    end

    def plan(schema)
      index_scan(schema) || table_scan(schema)
    end

    private

    def index_scan(schema)
      unless @order.size == 1
        return
      end

      index_name = schema.indexes.values.select { |idx| idx[:expr] == @order.first.expr }.map { |idx| idx[:name] }.first
      unless index_name
        return
      end

      plan = Kwery::Plan::IndexScan.new(@from, index_name, @order.first.order)

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
      if @order.size > 0
        plan = Kwery::Plan::Sort.new(
          lambda { |tup_a, tup_b|
            # => enum of orderby fields
            # => enum of ruby "spaceship" results (-1|0|1)
            # => take first non value that is not 0 (tup_a != tup_b)
            # => fall back to 0 if none found
            @order
              .lazy
              .map { |orderby|
                a = orderby.expr.call(tup_a)
                b = orderby.expr.call(tup_b)
                if orderby.order == :asc
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

    class OrderBy < Struct.new(:expr, :order)
    end
  end
end
