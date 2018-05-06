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

    class NoTableScanError < StandardError
    end
  end
end
