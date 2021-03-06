require 'csv'

module Kwery
  class Importer
    class Csv
      def initialize(schema)
        @schema = schema
      end

      def load(table_name, filename, type_map = {})
        file = File.open(filename)

        @schema.create_table(table_name)

        format = Kwery::Format::Csv.new(type_map)
        tups = format.load(file)

        tups.each do |tup|
          @schema.insert(table_name, tup)
        end
      end
    end
  end
end
