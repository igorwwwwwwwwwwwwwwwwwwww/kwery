require 'set'

module Kwery
  module Expr
    FN_TABLE = {
      upper: lambda { |x| x.upcase }
    }

    class Column < Struct.new(:name)
      def call(tup)
        tup[name]
      end
    end

    class Literal < Struct.new(:value)
      def call(tup)
        value
      end
    end

    class And < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) && right.call(tup)
      end
    end

    class Or < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) || right.call(tup)
      end
    end

    class In < Struct.new(:expr, :vals)
      def call(tup)
        vals.map { |val| val.call(tup) }.include?(expr.call(tup))
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

      def self.sarg_key
        :gt
      end
    end

    class Gte < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) >= right.call(tup)
      end

      def self.sarg_key
        :gte
      end
    end

    class Lt < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) < right.call(tup)
      end

      def self.sarg_key
        :lt
      end
    end

    class Lte < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) <= right.call(tup)
      end

      def self.sarg_key
        :lte
      end
    end

    class FnCall < Struct.new(:fn_name, :exprs)
      def call(tup)
        raise "no function named #{fn_name}" unless FN_TABLE[fn_name]

        args = exprs.map { |expr| expr.call(tup) }
        fn = FN_TABLE[fn_name]
        fn.call(*args)
      end
    end

    class AggCount < Struct.new(:exprs)
      def init
        0
      end

      def reduce(state, tup)
        state + 1
      end

      def render(state)
        {count: state}
      end
    end

    class AggAvg < Struct.new(:exprs)
      def init
        {count: 0, sum: 0}
      end

      def reduce(state, tup)
        val = exprs[0].call(tup)
        {
          count: state[:count] + 1,
          sum:   state[:sum] + val,
        }
      end

      def render(state)
        if state[:count] > 0
          {avg: state[:sum] / state[:count]}
        else
          {avg: 0}
        end
      end
    end

    AGG_FN_TABLE = {
      count: Kwery::Expr::AggCount,
      avg: Kwery::Expr::AggAvg
    }

    class IndexedExpr < Struct.new(:expr, :order)
      def reverse
        Kwery::Expr::IndexedExpr.new(
          expr,
          order == :asc ? :desc : :asc,
        )
      end
    end
  end
end
