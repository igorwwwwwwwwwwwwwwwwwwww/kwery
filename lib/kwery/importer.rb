require 'csv'

module Kwery
  class Importer
    def initialize(catalog, context)
      @catalog = catalog
      @context = context
    end

    def load(table_name, filename)
      unless File.exists?(filename)
        raise "could not find this file #{filename}"
      end

      table = @catalog.tables[table_name]

      csv = CSV.table(filename, converters: nil)
      csv.each do |row|
        tup = table.tuple(row)

        table_storage = @context[table_name]
        table_storage << tup
        tid = table_storage.size - 1

        table.indexes.each do |index_name|
          index = @context[index_name]

          key = @catalog.indexes[index_name].indexed_exprs.map(&:expr).map { |expr| expr.call(tup) }
          index.insert(key, tid)
        end
      end
    end
  end
end
