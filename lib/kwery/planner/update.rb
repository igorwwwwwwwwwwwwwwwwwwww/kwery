require 'set'

module Kwery
  class Planner
    class Insert
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        return unless Kwery::Query::Insert === @query

        tups = @query.values.lazy.map do |row|
          Hash[@query.keys.zip(row.map { |expr| expr.call({}) })]
        end

        plan = Kwery::Executor::UserQueryTups.new(tups)
        plan = Kwery::Executor::Insert.new(@query.into, plan)

        plan
      end
    end

    class Update
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        return unless Kwery::Query::Update === @query

        matcher = IndexMatcher.new(@schema, IndexMatcher::Query.new(
          table_name: @query.table,
          where: @query.where,
        ))
        candidates = matcher.match
        candidate = candidates.reject { |c| c.sorted }.first

        if candidate
          plan = Kwery::Executor::IndexScan.new(
            @query.table,
            candidate.index_name,
            candidate.sargs,
            :asc,
            @query.options,
          )
          plan = where(plan) if candidate.recheck
        else
          plan = Kwery::Executor::TableScan.new(
            @query.table,
            @query.options,
          )
          plan = where(plan)
        end

        update = lambda { |tup|
          @query.update.each do |k, expr|
            tup[k] = expr.call(tup)
          end
          tup
        }

        Kwery::Executor::Update.new(
          @query.table,
          update,
          plan
        )
      end

      private

      def where(plan)
        return plan unless @query.where.size > 0

        Kwery::Executor::Filter.new(
          lambda { |tup|
            @query.where.map { |cond| cond.call(tup) }.reduce(:&)
          },
          plan
        )
      end
    end

    class Delete
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        return unless Kwery::Query::Delete === @query

        matcher = IndexMatcher.new(@schema, IndexMatcher::Query.new(
          table_name: @query.from,
          where: @query.where,
        ))
        candidates = matcher.match
        candidate = candidates.reject { |c| c.sorted }.first

        if candidate
          plan = Kwery::Executor::IndexScan.new(
            @query.from,
            candidate.index_name,
            candidate.sargs,
            :asc,
            @query.options,
          )
          plan = where(plan) if candidate.recheck
        else
          plan = Kwery::Executor::TableScan.new(
            @query.from,
            @query.options,
          )
          plan = where(plan)
        end

        Kwery::Executor::Delete.new(@query.from, plan)
      end

      private

      def where(plan)
        return plan unless @query.where.size > 0

        Kwery::Executor::Filter.new(
          lambda { |tup|
            @query.where.map { |cond| cond.call(tup) }.reduce(:&)
          },
          plan
        )
      end
    end

    class Copy
      def initialize(schema, query)
        @schema = schema
        @query = query
      end

      def call
        return unless Kwery::Query::Copy === @query

        format = Kwery::Format::Json.new

        if @query.from == :stdin
          plan = Kwery::Executor::UserQueryStdin.new(format)
        else
          file = File.open(@query.from)
          plan = Kwery::Executor::UserQueryFile.new(file, format)
        end

        plan = Kwery::Executor::Insert.new(@query.table, plan)
        plan
      end
    end
  end
end
