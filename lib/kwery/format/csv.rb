require 'csv'

# TODO: consider using fastercsv
# TODO: find good solution for type conversion (type map is unused)
# TODO: consider deprecating in favour of json

module Kwery
  module Format
    class Csv
      def initialize(type_map = {})
        @type_map = type_map
      end

      def load(file, context = nil)
        CSV.table(file).map(&:to_h)
      end
    end
  end
end
