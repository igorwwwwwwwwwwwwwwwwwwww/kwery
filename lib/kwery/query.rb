require 'set'

module Kwery
  class Query
    attr_accessor :select, :from, :where, :order_by, :limit

    def initialize(select:, from:, where: nil, order_by: [], limit: nil)
      @select = select
      @from = from
      @where = where
      @order_by = order_by
      @limit = limit
    end

    def plan(catalog)
      Optimizer.new(catalog, self).call
    end
  end
end
