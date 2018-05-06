# TODO: rename to catalog

module Kwery
  class Schema
    attr_accessor :tables

    def initialize
      @tables = {}
    end

    def table(name)
      @tables[name] = Kwery::Schema::Table.new(name)

      yield @tables[name] if block_given?

      @tables[name]
    end

    class Table
      attr_accessor :columns
      attr_accessor :indexes

      def initialize(name)
        @name = name
        @columns = {}
        @indexes = {}
      end

      def column(name, type)
        @columns[name] = Kwery::Schema::Column.new(name, type)

        self
      end

      def index(name, *specs)
        columns = specs.map { |spec|
          table_name, column_name, order = spec
          Kwery::Query::OrderedField.new(
            Kwery::Query::Field.new(column_name),
            order,
          )
        }
        @indexes[name] = Kwery::Schema::Index.new(name, columns)

        self
      end

      def tuple(row)
        tup = {}
        @columns.each do |_, field|
          name = field.name
          type = field.type
          tup[name] = apply_type(row[name], type)
        end
        tup
      end

      private

      def apply_type(v, type)
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
    end

    class Column < Struct.new(:name, :type)
    end

    class Index < Struct.new(:name, :columns)
      def columns_flipped
        columns.map { |ordered_field|
          Kwery::Query::OrderedField.new(
            ordered_field.expr,
            ordered_field.order == :asc ? :desc : :asc,
          )
        }
      end
    end
  end
end
