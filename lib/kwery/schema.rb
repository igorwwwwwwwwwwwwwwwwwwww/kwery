module Kwery
  class Schema
    attr_accessor :fields
    attr_accessor :indexes

    def initialize
      @fields = {}
      @indexes = {}
    end

    def column(name, type)
      @fields[name] = { name: name, type: type }
    end

    def index(name, expr)
      @indexes[name] = { name: name, expr: expr }
    end

    def tuple(row)
      tup = {}
      @fields.each do |_, field|
        name = field[:name]
        type = field[:type]
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
end
