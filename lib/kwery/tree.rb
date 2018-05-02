require 'binary_search_tree'

module Kwery
  class Tree
    def initialize
      @bst = BinarySearchTree.new
    end

    def insert(k, v)
      @bst.insert(k, v)
    end

    def scan(order = :asc)
      if order == :asc
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
