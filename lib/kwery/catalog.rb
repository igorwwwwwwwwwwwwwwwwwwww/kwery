module Kwery
  class Catalog
    attr_accessor :tables
    attr_accessor :indexes

    def initialize
      @tables = {}
      @indexes = {}
    end

    def table(name, table)
      @tables[name] = table
    end

    def index(name, index)
      @indexes[name] = index
    end

    def new_schema
      Kwery::Schema.new(self)
    end

    class Table
      attr_accessor :indexes

      def initialize(indexes: [])
        @indexes = indexes
      end
    end

    class Index
      attr_accessor :table, :indexed_exprs

      def initialize(table, indexed_exprs)
        @table = table
        @indexed_exprs = indexed_exprs
      end

      def indexed_exprs_reverse
        indexed_exprs.map(&:reverse)
      end
    end

    class IndexedExpr < Struct.new(:expr, :order)
      def reverse
        Kwery::Catalog::IndexedExpr.new(
          expr,
          order == :asc ? :desc : :asc,
        )
      end
    end
  end
end
