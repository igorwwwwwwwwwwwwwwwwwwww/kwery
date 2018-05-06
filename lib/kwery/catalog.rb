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

    def self.apply_type(v, type)
      return nil if v.nil?
      case type
      when :integer
        Integer(v)
      when :string
        v
      when :boolean
        v.downcase == 'true' ? true : false
      else
        raise "unknown type #{type}"
      end
    end

    class Table
      attr_accessor :columns
      attr_accessor :indexes

      def initialize(columns:, indexes:)
        @columns = columns
        @indexes = indexes
      end

      def tuple(row)
        tup = {}
        @columns.each do |name, column|
          type = column.type
          tup[name] = Catalog.apply_type(row[name], type)
        end
        tup
      end
    end

    class Column < Struct.new(:type)
    end

    class Index < Struct.new(:indexed_exprs)
      def reverse
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
