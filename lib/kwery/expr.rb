require 'set'

module Kwery
  module Expr
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

    class Upper < Struct.new(:expr)
      def call(tup)
        expr.call(tup).upcase
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
