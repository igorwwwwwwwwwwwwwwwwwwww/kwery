require 'set'

module Kwery
  class Query
    attr_accessor :select, :select_star, :from, :where, :order_by, :limit, :options

    def initialize(select:, select_star: false, from: nil, where: [], order_by: [], limit: nil, options: {})
      @select = select
      @select_star = select_star
      @from = from
      @where = where
      @order_by = order_by
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
      [select, select_star, from, where, order_by, limit, options]
    end
  end
end
