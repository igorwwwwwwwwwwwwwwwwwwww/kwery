require 'csv'

# TODO: consider using fastercsv
# TODO: find good solution for type conversion

module Kwery
  class Format
    class Csv
      def load(file, type_map = {}, context = nil)
        CSV.table(file).map(&:to_h)
      end
    end
  end
end
