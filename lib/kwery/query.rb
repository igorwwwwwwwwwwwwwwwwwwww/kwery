require 'set'

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
    end
  end
end
