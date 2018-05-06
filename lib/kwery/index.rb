require 'binary_search_tree'
require 'set'

module Kwery
  class Index
    def initialize(comparator: nil)
      @bst = BinarySearchTree.new(comparator: comparator)
    end

    def insert(k, v)
      vals = @bst.find(k)&.value
      unless vals
        vals = Set.new
        @bst.insert(k, vals)
      end
      vals << v
    end

    def scan(scan_order = :asc, sargs = {})
      if sargs[:eq]
        node = @bst.find(sargs[:eq])
        return node ? [node.value] : []
      end

      if sargs.size > 0
        return scan_leaf_cond(scan_order) do |key|
          sargs_cond(key, sargs)
        end
      end

      scan_leaf(scan_order)
    end

    def scan_leaf(scan_order)
      scan_order == :asc ? @bst.scan_leaf_asc : @bst.scan_leaf_desc
    end

    def scan_leaf_cond(scan_order, &block)
      scan_order == :asc ? @bst.scan_leaf_asc_cond(&block) : @bst.scan_leaf_desc_cond(&block)
    end

    def sargs_cond(key, sargs)
      return false unless sargs[:gt].nil?  || @bst.comparator.call(key, sargs[:gt])  > 0
      return false unless sargs[:gte].nil? || @bst.comparator.call(key, sargs[:gte]) >= 0
      return false unless sargs[:lt].nil?  || @bst.comparator.call(key, sargs[:lt])  < 0
      return false unless sargs[:lte].nil? || @bst.comparator.call(key, sargs[:lte]) <= 0
      return true
    end
  end
end
