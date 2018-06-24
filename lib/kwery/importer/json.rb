require 'json'

module Kwery
  class Importer
    class Json
      def initialize(schema)
        @schema = schema
      end

      def load(table_name, filename)
        unless File.exists?(filename)
          raise "could not find this file #{filename}"
        end

        @schema.create_table(table_name)

        File.readlines(filename).each do |line|
          tup = JSON.parse(line, symbolize_names: true)
          @schema.insert(table_name, tup)
        end
      end
    end
  end
end
