module Kwery
  module Executor
    class NoTableScanError < StandardError
    end

    class Context
      attr_accessor :schema, :stdin, :stats

      def initialize(schema, stdin = nil, stats = {})
        @schema = schema
        @stdin = stdin
        @stats = stats
      end

      def increment(key, count = 1)
        stats[key] ||= 0
        stats[key] += count
      end
    end

    # TODO: attempt to reconstruct tuples from the index key
    class IndexOnlyScan
      def initialize(index_name, sargs = {}, scan_order = :asc, options = {})
        @index_name = index_name
        @sargs = sargs
        @scan_order = scan_order
        @options = options
      end

      def call(context)
        context.schema
          .index_scan(@index_name, @sargs, @scan_order, context)
          .map {|k,tids|
            { _key: k, _count: tids.size, _tids: tids }
          }
      end

      def explain(context)
        [self.class, @index_name, @sargs]
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
          .flat_map {|k,tids|
            tids.map { |tid|
              context.increment :index_tuples_fetched
              context.schema.fetch(@table_name, tid)
            }
          }
      end

      def explain(context)
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

      def explain(context)
        self.class
      end
    end

    class EmptyScan
      def call(context)
        [{}]
      end

      def explain(context)
        self.class
      end
    end

    class UserQueryTups
      def initialize(tups)
        @tups = tups
      end

      def call(context)
        @tups.lazy
      end

      def explain(context)
        self.class
      end
    end

    class UserQueryFile
      def initialize(file, format)
        @file = file
        @format = format
      end

      def call(context)
        @format.load(file, {}, context)
      end

      def explain(context)
        self.class
      end
    end

    class UserQueryStdin
      def initialize(format)
        @format = format
      end

      def call(context)
        @format.load(context.stdin, {}, context)
      end

      def explain(context)
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

      def explain(context)
        [self.class, @plan.explain(context)]
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

      def explain(context)
        [self.class, @plan.explain(context)]
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

      def explain(context)
        [self.class, @plan.explain(context)]
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

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class WithoutTid
      def initialize(plan)
        @plan = plan
      end

      def call(context)
        @plan.call(context).map do |tup|
          tup.dup.tap { |t| t.delete(:_tid) }
        end
      end

      def explain(context)
        [self.class, @plan.explain(context)]
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

      def explain(context)
        [self.class, @agg.class, @plan.explain(context)]
      end
    end

    class HashAggregate
      def initialize(k, agg, group_by, group_keys, plan)
        @k = k
        @agg = agg
        @group_by = group_by
        @group_keys = group_keys
        @plan = plan
      end

      def call(context)
        states = @plan.call(context)
          .group_by(&@group_by)
          .map { |k, vs| [k, vs.reduce(@agg.init, &@agg.method(:reduce))] }
          .to_h

        states.map do |k, state|
          tup = {}
          tup.merge!(Hash[@group_keys.zip(k)].reject { |k| k.nil? })
          tup.merge!({ @k => @agg.render(state) })
          tup
        end
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class PartialAggregate
      def initialize(k, agg, plan)
        @k = k
        @agg = agg
        @plan = plan
      end

      def call(context)
        state = @plan.call(context).reduce(@agg.init, &@agg.method(:reduce))
        [{ @k => state }]
      end

      def explain(context)
        [self.class, @agg.class, @plan.explain(context)]
      end
    end

    class CombineAggregates
      def initialize(k, agg, plan)
        @k = k
        @agg = agg
        @plan = plan
      end

      def call(context)
        states = @plan.call(context).map { |tup| tup[@k] }
        state = @agg.combine(states)
        val = @agg.render(state)
        [{ @k => val }]
      end

      def explain(context)
        [self.class, @agg.class, @plan.explain(context)]
      end
    end

    class MergeCounts
      def initialize(plan)
        @plan = plan
      end

      def call(context)
        count = @plan.call(context).reduce(0) do |sum, tup|
          sum + tup[:count]
        end
        [{ count: count }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class Append
      def initialize(plans)
        @plans = plans
      end

      def call(context)
        Enumerator.new do |y|
          @plans.each do |plan|
            plan.call(context).each do |tup|
              y << tup
            end
          end
        end
      end

      def explain(context)
        [self.class, @plans.map { |p| p.explain(context) }]
      end
    end

    # queries is a map from backend to sql
    # e.g.
    #   { "http://localhost:8000": "select * from users" }
    class RemoteBatch
      def initialize(queries, client_opts = {})
        @queries = queries
        @client_opts = client_opts
      end

      def call(context)
        client = Kwery::Client::Batch.new
        results = client.query(@queries, @client_opts, context)
        Enumerator.new do |y|
          results.each do |result|
            result[:data].each do |tup|
              y << tup
            end
          end
        end
      end

      # TODO: verbose mode that includes generated queries?
      def explain(context)
        client = Kwery::Client::Batch.new
        results = client.query(@queries, {}, context)
        remote_explain = results.map do |result|
          result[:data].first[:explain]
        end
        remote_explain_map = @queries.keys.zip(remote_explain).to_h

        [self.class, @client_opts, remote_explain_map]
      end
    end

    class Insert
      def initialize(table_name, plan)
        @table_name = table_name
        @plan = plan
      end

      def call(context)
        tups = @plan.call(context)
        count = context.schema.bulk_insert(@table_name, tups)

        [{
          count: count,
        }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class Update
      def initialize(table_name, update, plan)
        @table_name = table_name
        @update = update
        @plan = plan
      end

      def call(context)
        # TODO fetch tids and update the index separately to prevent
        #      issues with the index changing while we are iterating
        #      over it

        count = 0
        @plan.call(context).each do |tup|
          context.schema.update(@table_name, tup, @update)
          count += 1
        end

        [{
          count: count,
        }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class Delete
      def initialize(table_name, plan)
        @table_name = table_name
        @plan = plan
      end

      def call(context)
        count = 0
        @plan.call(context).each do |tup|
          context.schema.delete(@table_name, tup)
          count += 1
        end

        [{
          count: count,
        }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end

    class Explain
      def initialize(plan)
        @plan = plan
      end

      def call(context)
        [{
          _pretty: true,
          explain: @plan.explain(context),
        }]
      end

      def explain(context)
        [self.class, @plan.explain(context)]
      end
    end
  end
end
