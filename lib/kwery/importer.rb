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
        @schema.insert(table_name, row)
      end
    end
  end
end
