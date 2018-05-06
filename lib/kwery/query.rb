require 'set'

module Kwery
  class Query
    attr_accessor :select, :from, :where, :order_by, :limit

    def initialize(select:, from:, where: nil, order_by: [], limit: nil)
      @select = select
      @from = from
      @where = where
      @order_by = order_by
      @limit = limit
    end

    def plan(schema)
      Optimizer.new(schema, self).call
    end

    class Field < Struct.new(:column)
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

    class NoTableScanError < StandardError
    end
  end
end
