module Kwery
  module Executor
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
      def initialize(table_name, index_name, sargs = {}, scan_order = :asc, options = {})
        @table_name = table_name
        @index_name = index_name
        @sargs = sargs
        @scan_order = scan_order
        @options = options
      end

      def call(context)
        context.schema
          .index_scan(@index_name, @sargs, @scan_order, context)
          .flat_map {|tids|
            tids.map { |tid|
              context.increment :index_tuples_fetched
              context.schema.fetch(@table_name, tid)
            }
          }
      end

      def explain
        [self.class, @index_name, @sargs]
      end
    end

    class TableScan
      def initialize(table_name, options = {})
        @table_name = table_name
        @options = options
      end

      def call(context)
        if @options[:notablescan]
          # a notable scan indeed
          raise Kwery::Executor::NoTableScanError.new("query resulted in table scan")
        end

        context.schema.table_scan(@table_name).map {|tup|
          context.increment :table_tuples_scanned
          tup
        }
      end

      def explain
        self.class
      end
    end

    class EmptyScan
      def call(context)
        [{}]
      end

      def explain
        self.class
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

      def explain
        [self.class, @plan.explain]
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

      def explain
        [self.class, @plan.explain]
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

      def explain
        [self.class, @plan.explain]
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

      def explain
        [self.class, @plan.explain]
      end
    end

    # TODO: support multiple aggregations side-by-side
    #   e.g. select count(*), avg(experience) from pokemon

    class Aggregate
      def initialize(k, agg, plan)
        @k = k
        @agg = agg
        @plan = plan
      end

      def call(context)
        state = @plan.call(context).reduce(@agg.init, &@agg.method(:reduce))
        val = @agg.render(state)
        [{ @k => val }]
      end

      def explain
        [self.class, @plan.explain]
      end
    end

    class HashAggregate
      def initialize(k, agg, group_name, group_by, plan)
        @k = k
        @agg = agg
        @group_name = group_name
        @group_by = group_by
        @plan = plan
      end

      def call(context)
        states = @plan.call(context)
          .group_by(&@group_by)
          .map { |k, vs| [k, vs.reduce(@agg.init, &@agg.method(:reduce))] }
          .to_h

        states.map do |k, state|
          tup = { @k => @agg.render(state) }
          tup[@group_name] = k if @group_name
          tup
        end
      end

      def explain
        [self.class, @plan.explain]
      end
    end

    class AggregateCount < Struct.new(:exprs)
      def init
        0
      end

      def reduce(state, tup)
        state + 1
      end

      def render(state)
        state
      end
    end

    class AggregateAvg < Struct.new(:exprs)
      def init
        {count: 0, sum: 0}
      end

      def reduce(state, tup)
        val = exprs[0].call(tup)
        raise "avg: invalid expr #{exprs[0]} on called on tup #{tup}" unless val
        {
          count: state[:count] + 1,
          sum:   state[:sum] + val,
        }
      end

      def render(state)
        if state[:count] > 0
          state[:sum] / state[:count]
        else
          0
        end
      end
    end

    AGG_FN_TABLE = {
      count: Kwery::Executor::AggregateCount,
      avg:   Kwery::Executor::AggregateAvg,
    }

    class Explain
      def initialize(plan)
        @plan = plan
      end

      def call(context)
        [{
          _pretty: true,
          explain: @plan.explain,
        }]
      end

      def explain
        [self.class, @plan.explain]
      end
    end
  end
end
