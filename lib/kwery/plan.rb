module Kwery
  module Plan
    class NoTableScanError < StandardError
    end

    class Context
      attr_accessor :schema, :stats

      def initialize(schema, stats = {})
        @schema = schema
        @stats = stats
      end

      def increment(key, count = 1)
        stats[key] ||= 0
        stats[key] += count
      end
    end

    class IndexScan
      # sargs = search args
      # see: Access Path Selection in a Relational Database Management System
      def initialize(table_name, index_name, scan_order = :asc, sargs = {})
        @table_name = table_name
        @index_name = index_name
        @scan_order = scan_order
        @sargs = sargs
      end

      def call(context)
        context.schema.index_scan(@index_name, @scan_order, @sargs).flat_map {|tids|
          tids.map { |tid|
            context.increment :index_tuples_scanned
            context.schema.fetch(@table_name, tid)
          }
        }
      end
    end

    class TableScan
      def initialize(table_name)
        @table_name = table_name
      end

      def call(context)
        context.schema.table_scan(@table_name).map {|tup|
          context.increment :table_tuples_scanned
          tup
        }
      end
    end

    class Filter
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

    class Sort
      def initialize(comp, plan)
        @comp = comp
        @plan = plan
      end

      def call(context)
        @plan.call(context).sort(&@comp)
      end
    end

    class Project
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
