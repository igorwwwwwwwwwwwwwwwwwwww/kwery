require 'set'

module Kwery
  module Expr
    FN_TABLE = {
      upper: lambda { |x| x.upcase }
    }

    AGG_FN_TABLE = {
      count: lambda { |x| Kwery::Executor::AggregateCount.new(x) },
      sum:   lambda { |x| Kwery::Executor::AggregateSum.new(x) },
      max:   lambda { |x| Kwery::Executor::AggregateMax.new(x) },
      avg:   lambda { |x| Kwery::Executor::AggregateAvg.new(x) },
    }

    class Column < Struct.new(:name)
      def call(tup)
        tup[name]
      end

      def to_s
        name.to_s
      end
    end

    class Literal < Struct.new(:value)
      def call(tup)
        value
      end

      def to_s
        value.to_s
      end
    end

    class And < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) && right.call(tup)
      end

      def to_s
        "#{left.to_s} && #{right.to_s}"
      end
    end

    class Or < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) || right.call(tup)
      end

      def to_s
        "#{left.to_s} || #{right.to_s}"
      end
    end

    class In < Struct.new(:expr, :vals)
      def call(tup)
        vals.map { |val| val.call(tup) }.include?(expr.call(tup))
      end

      def to_s
        "#{expr.to_s} IN (#{vals.map(&:to_s).join(', ')})"
      end
    end

    class Eq < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) == right.call(tup)
      end

      def to_s
        "#{left.to_s} = #{right.to_s}"
      end
    end

    class Gt < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) > right.call(tup)
      end

      def self.sarg_key
        :gt
      end

      def to_s
        "#{left.to_s} > #{right.to_s}"
      end
    end

    class Gte < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) >= right.call(tup)
      end

      def self.sarg_key
        :gte
      end

      def to_s
        "#{left.to_s} >= #{right.to_s}"
      end
    end

    class Lt < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) < right.call(tup)
      end

      def self.sarg_key
        :lt
      end

      def to_s
        "#{left.to_s} < #{right.to_s}"
      end
    end

    class Lte < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) <= right.call(tup)
      end

      def self.sarg_key
        :lte
      end

      def to_s
        "#{left.to_s} <= #{right.to_s}"
      end
    end

    class Neq < Struct.new(:left, :right)
      def call(tup)
        left.call(tup) != right.call(tup)
      end

      def to_s
        "#{left.to_s} <> #{right.to_s}"
      end
    end

    class FnCall < Struct.new(:fn_name, :exprs)
      def call(tup)
        raise "no function named #{fn_name}" unless FN_TABLE[fn_name]

        args = exprs.map { |expr| expr.call(tup) }
        fn = FN_TABLE[fn_name]
        fn.call(*args)
      end

      def to_s
        "#{fn_name.to_s}(#{exprs.map(&:to_s).join(', ')})"
      end
    end

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
