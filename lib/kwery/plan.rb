module Kwery
  module Plan
    class IndexScan
      include Enumerable

      def initialize(table_name, index_name, order = :asc)
        @table_name = table_name
        @index_name = index_name
        @order = order
      end

      def call(context)
        index = context[@index_name]
        table = context[@table_name]
        index.scan(@order).lazy.map {|tid|
          tup = table[tid]
          tup
        }
      end
    end

    class TableScan
      include Enumerable

      def initialize(table_name)
        @table_name = table_name
      end

      def call(context)
        table = context[@table_name]
        table # table is already an enumerable of tuples

        table.lazy
      end
    end

    class Filter
      include Enumerable

      def initialize(pred, plan)
        @pred = pred
        @plan = plan
      end

      def call(context)
        @plan.call(context).select(&@pred)
      end
    end

    class Limit
      def initialize(limit, plan)
        @limit = limit
        @plan = plan
      end

      def call(context)
        @plan.call(context).take(@limit)
      end
    end

    class Project
      include Enumerable

      def initialize(proj, plan)
        @proj = proj
        @plan = plan
      end

      def call(context)
        @plan.call(context).map(&@proj)
      end
    end
  end
end
