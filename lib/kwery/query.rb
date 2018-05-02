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
      unless @order.size == 1
        raise 'only single-field index scans supported by query planner'
      end

      index_name = schema.indexes.values.select { |idx| idx[:expr] == @order.first.expr }.map { |idx| idx[:name] }.first
      unless index_name
        raise 'no suitable index found by planner'
      end

      plan = Kwery::Plan::IndexScan.new(@from, index_name, @order.first.order)

      if @where
        plan = Kwery::Plan::Filter.new(
          lambda { |tup| @where.call(tup) },
          plan
        )
      end

      if @limit
        plan = Kwery::Plan::Limit.new(@limit, plan)
      end

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
