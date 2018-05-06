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
      scan_leaf_cond(scan_order, sargs)
    end

    def scan_leaf(scan_order)
      scan_order == :asc ? @bst.scan_leaf_asc : @bst.scan_leaf_desc
    end

    def scan_leaf_cond(scan_order, sargs)
      scan_order == :asc ? @bst.scan_leaf_asc_cond(@bst.root, sargs) : @bst.scan_leaf_desc_cond(@bst.root, sargs)
    end
  end
end
