module Kwery
  module Plan
    class IndexScan
      def initialize(table_name, index_name, scan_order = :asc)
        @table_name = table_name
        @index_name = index_name
        @scan_order = scan_order
      end

      def call(schema)
        index = schema[@index_name]
        table = schema[@table_name]
        index.scan(@scan_order).lazy.flat_map {|tids|
          tids.map { |tid|
            tup = table[tid]
            tup
          }
        }
      end
    end

    class TableScan
      def initialize(table_name)
        @table_name = table_name
      end

      def call(schema)
        table = schema[@table_name]
        table # table is already an enumerable of tuples

        table.lazy
      end
    end

    class Filter
      def initialize(pred, plan)
        @pred = pred
        @plan = plan
      end

      def call(schema)
        @plan.call(schema).select(&@pred)
      end
    end

    class Limit
      def initialize(limit, plan)
        @limit = limit
        @plan = plan
      end

      def call(schema)
        @plan.call(schema).take(@limit)
      end
    end

    class Sort
      def initialize(comp, plan)
        @comp = comp
        @plan = plan
      end

      def call(schema)
        @plan.call(schema).sort(&@comp)
      end
    end

    class Project
      def initialize(proj, plan)
        @proj = proj
        @plan = plan
      end

      def call(schema)
        @plan.call(schema).map(&@proj)
      end
    end
  end
end
