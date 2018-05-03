module Kwery
  class Schema
    attr_accessor :columns
    attr_accessor :indexes

    def initialize
      @columns = {}
      @indexes = {}
    end

    def column(name, type)
      @columns[name] = Kwery::Schema::Column.new(name, type)
    end

    def index(name, *specs)
      columns = specs.map { |spec|
        table_name, column_name, order = spec
        Kwery::Query::OrderedField.new(
          Kwery::Query::Field.new(table_name, column_name),
          order,
        )
      }
      @indexes[name] = Kwery::Schema::Index.new(name, columns)
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
