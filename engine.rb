require 'binary_search_tree'
require 'csv'

module Kwery
  class Tree
    def initialize
      @bst = BinarySearchTree.new
    end

    def insert(k, v)
      @bst.insert(k, v)
    end

    def scan(order = :asc, &block)
      if order == :asc
        scan_leaf_asc(@bst.root, &block)
      else
        scan_leaf_desc(@bst.root, &block)
      end
    end

    private

    def scan_leaf_asc(leaf, &block)
      return if leaf.nil?
      scan_leaf_asc(leaf.left, &block)
      block.call([leaf.key, leaf.value])
      scan_leaf_asc(leaf.right, &block)
    end

    def scan_leaf_desc(leaf, &block)
      return if leaf.nil?
      scan_leaf_desc(leaf.right, &block)
      block.call([leaf.key, leaf.value])
      scan_leaf_desc(leaf.left, &block)
    end
  end
end

table = Kwery::Tree.new

csv = CSV.table('users.csv')
csv.each do |row|
  table.insert(row[:id], row.to_h)
end

# puts table.find(23).value

table.scan(:desc) do |k, tup|
  puts tup
end

# Limit(10)
#   Filter('active = true')
#     IndexScan('users', 'id', nil, nil, 'desc', ['name', 'active'])
