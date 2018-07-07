require 'set'

# TODO: shared planner object?

module Kwery
  module Query
    class Select
      attr_accessor :select, :from, :where, :order_by, :group_by, :limit, :options

      def initialize(select:, from: nil, where: [], order_by: [], group_by: [], limit: nil, options: {})
        @select = select
        @from = from
        @where = where
        @order_by = order_by
        @group_by = group_by
        @limit = limit
        @options = options
      end

      def plan(schema)
        Planner.new(schema, self).call
      end

       def ==(other)
         other.respond_to?(:parts) && self.parts == other.parts
       end

      def parts
        [select, from, where, order_by, group_by, limit, options]
      end

      def to_sql
        parts = []
        parts << "EXPLAIN" if options[:explain]
        parts << "SELECT #{select.join(', ')}"
        parts << "FROM #{from}"                 if from
        parts << "WHERE #{where}"               if where.size > 0
        parts << "ORDER BY #{order_by}"         if order_by.size > 0
        parts << "GROUP BY #{group_by}"         if group_by.size > 0
        parts << "LIMIT #{limit}"               if limit
        parts.join(' ')
      end
    end

    class Insert
      attr_accessor :into, :keys, :values, :options

      def initialize(into:, keys:, values:, options: {})
        @into = into
        @keys = keys
        @values = values
        @options = options
      end

      def plan(schema)
        Planner.new(schema, self).call
      end

       def ==(other)
         other.respond_to?(:parts) && self.parts == other.parts
       end

      def parts
        [into, keys, values, options]
      end

      def to_sql
        # TODO: string quotes and escaping
        values_parts = values.map do |row|
          '(' + row.join(', ') + ')'
        end
        values_sql = values_parts.join(', ')

        parts = []
        parts << "EXPLAIN" if options[:explain]
        parts << "INSERT INTO #{into}"
        parts << "(#{keys.join(', ')})"
        parts << "VALUES #{values_sql}"
        parts.join(' ')
      end
    end

    class Update
      attr_accessor :table, :update, :where, :options

      def initialize(table:, update:, where: [], options: {})
        @table = table
        @update = update
        @where = where
        @options = options
      end

      def plan(schema)
        Planner.new(schema, self).call
      end

       def ==(other)
         other.respond_to?(:parts) && self.parts == other.parts
       end

      def parts
        [table, update, where, options]
      end

      def to_sql
        update_parts = update.map do |k, v|
          "#{k} = #{v}"
        end
        update_sql = update_parts.join(', ')

        parts = []
        parts << "EXPLAIN"                    if options[:explain]
        parts << "UPDATE #{table}"
        parts << "SET #{update_sql}"
        parts << "WHERE #{where.join(', ')}"  if where.size > 0
        parts.join(' ')
      end
    end

    class Delete
      attr_accessor :from, :where, :options

      def initialize(from:, where: [], options: {})
        @from = from
        @where = where
        @options = options
      end

      def plan(schema)
        Planner.new(schema, self).call
      end

       def ==(other)
         other.respond_to?(:parts) && self.parts == other.parts
       end

      def parts
        [from, where, options]
      end

      def to_sql
        parts = []
        parts << "EXPLAIN"                    if options[:explain]
        parts << "DELETE FROM #{from}"
        parts << "WHERE #{where.join(', ')}"  if where.size > 0
        parts.join(' ')
      end
    end

    class Copy
      attr_accessor :table, :from, :options

      def initialize(table:, from:, options: {})
        @table = table
        @from = from
        @options = options
      end

      def plan(schema)
        Planner.new(schema, self).call
      end

       def ==(other)
         other.respond_to?(:parts) && self.parts == other.parts
       end

      def parts
        [table, from, options]
      end

      def to_sql
        # TODO: string quotes and escaping
        parts = []
        parts << "EXPLAIN"                    if options[:explain]
        parts << "COPY #{table}"
        parts << "FROM #{from}"
        parts.join(' ')
      end
    end
  end
end
