require 'set'

module Kwery
  class Query
    attr_accessor :select, :from, :where, :order_by, :limit, :options

    def initialize(select:, from:, where: [], order_by: [], limit: nil, options: {})
      @select = select
      @from = from
      @where = where
      @order_by = order_by
      @limit = limit
      @options = options
    end

    def plan(catalog)
      Planner.new(catalog, self).call
    end
  end
end
