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
        node = @bst.find(sargs[:eq]) rescue nil
        return node ? [node.value] : []
      end

      if scan_order == :asc
        scan_leaf_asc(@bst.root)
      else
        scan_leaf_desc(@bst.root)
      end
    end

    private

    def scan_leaf_asc(leaf)
      return [] if leaf.nil?
      Enumerator.new do |y|
        scan_leaf_asc(leaf.left).each do |v|
          y << v
        end
        y << leaf.value
        scan_leaf_asc(leaf.right).each do |v|
          y << v
        end
      end
    end

    def scan_leaf_desc(leaf)
      Enumerator.new do |y|
        if leaf.right
          scan_leaf_desc(leaf.right).each do |v|
            y << v
          end
        end
        y << leaf.value
        if leaf.left
          scan_leaf_desc(leaf.left).each do |v|
            y << v
          end
        end
      end
    end
  end
end
