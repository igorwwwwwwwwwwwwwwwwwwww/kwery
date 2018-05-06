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

      if sargs[:gt]
        node = @bst.find_insert_point(sargs[:gt])
        return @bst.scan_leaf_gt(sargs[:gt], node)
      end

      if sargs[:gte]
        node = @bst.find_insert_point(sargs[:gte])
        @bst.print_tree
        return @bst.scan_leaf_gte(sargs[:gte], node)
      end

      scan_order == :asc ? @bst.scan_leaf_asc : @bst.scan_leaf_desc
    end
  end
end
