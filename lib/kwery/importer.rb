require 'csv'

module Kwery
  class Importer
    def initialize(schema)
      @schema = schema
    end

    def load(table_name, filename, type_map = {})
      unless File.exists?(filename)
        raise "could not find this file #{filename}"
      end

      csv = CSV.table(filename, converters: nil)
      csv.each do |row|
        @schema.insert(table_name, tup(row, type_map))
      end
    end

    def tup(row, type_map)
      row.map { |k, v|
        case type_map[k]
        when :integer
          v = Integer(v)
        when :boolean
          v = ['true', 'TRUE'].include?(v)
        when :string
          # keep as string
        when nil
          # no entry in type_map, keep as string
        else
          raise "unknown type #{type_map[k]}"
        end
        [k, v]
      }.to_h
    end
  end
end
