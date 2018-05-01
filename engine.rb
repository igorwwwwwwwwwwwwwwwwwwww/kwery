require 'binary_search_tree'
require 'csv'

ORDER_ASC = 0
ORDER_DESC = 1

def self.scan(table, order = ORDER_ASC, &block)
  if order == ORDER_ASC
    scan_leaf_asc(table.root, &block)
  else
    scan_leaf_desc(table.root, &block)
  end
end

def self.scan_leaf_asc(leaf, &block)
  return if leaf.nil?
  scan_leaf_asc(leaf.left, &block)
  block.call([leaf.key, leaf.value])
  scan_leaf_asc(leaf.right, &block)
end

def self.scan_leaf_desc(leaf, &block)
  return if leaf.nil?
  scan_leaf_desc(leaf.right, &block)
  block.call([leaf.key, leaf.value])
  scan_leaf_desc(leaf.left, &block)
end

table = BinarySearchTree.new

csv = CSV.table('users.csv')
csv.each do |row|
  table.insert row[:id], row.to_h
end

# puts table.find(23).value

scan(table, ORDER_DESC) do |k, tup|
  puts tup
end

# Limit(10)
#   Filter('active = true')
#     IndexScan('users', 'id', nil, nil, 'desc', ['name', 'active'])
