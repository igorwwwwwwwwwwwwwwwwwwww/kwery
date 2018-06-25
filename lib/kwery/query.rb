require 'set'

module Kwery
  class Query
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
end
