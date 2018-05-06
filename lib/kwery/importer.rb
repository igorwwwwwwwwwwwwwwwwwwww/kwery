require 'csv'

module Kwery
  class Importer
    def initialize(catalog, schema)
      @catalog = catalog
      @schema = schema
    end

    def load(table_name, filename)
      unless File.exists?(filename)
        raise "could not find this file #{filename}"
      end

      table = @catalog.tables[table_name]

      csv = CSV.table(filename, converters: nil)
      csv.each do |row|
        tup = table.tuple(row)

        relation = @schema[table_name]
        relation << tup
        tid = relation.size - 1

        table.indexes.each do |index_name|
          index = @schema[index_name]

          key = @catalog.indexes[index_name].indexed_exprs.map(&:expr).map { |expr| expr.call(tup) }
          index.insert(key, tid)
        end
      end
    end
  end
end
