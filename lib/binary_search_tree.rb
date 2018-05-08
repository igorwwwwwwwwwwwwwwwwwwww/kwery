# this is a slightly adapted binary search tree,
# taken from the `binary_search_tree` gem by
# Misha Conway.
#
# the following changes have been made:
# * support custom comparator
# * scan_leaf   to scan index based on sargs (search args)
# * print_tree  print the full tree for debugging

class BinarySearchTree
  attr_reader :size, :root, :comparator

  def balanced?
    -1 == compute_and_check_height(@root) ? false : true
  end

  def initialize(comparator: nil, logger: nil)
    @comparator = comparator || lambda { |a, b| a <=> b }
    @logger = logger
    clear
  end

  def clear
    @root = nil
    @size = 0
  end

  def empty?
    @root.nil?
  end

  def find(key)
    @num_comparisons = 0
    node = locate key, @root
    @logger.debug "find operation completed in #{@num_comparisons} lookups..." if @logger # rubocop:disable Metrics/LineLength
    node
  end

  def find_value(value)
    find_value_ex @root, value
  end

  def min
    @min ||= locate_min @root
  end

  def max
    @max ||= locate_max @root
  end

  def insert(element, value)
    put element, value, @root, nil
  end

  def remove(node_or_key)
    delete node_or_key
  end

  def remove_min
    delete min
  end

  def nodes
    @nodes = []
    serialize_nodes @root
    @nodes
  end

  def ==(other)
    compare @root, other.root
  end

  def scan_leaf(context, leaf = @root, sargs = {}, scan_order = :asc)
    scan_order == :asc ? scan_leaf_asc(context, leaf, sargs) : scan_leaf_desc(context, leaf, sargs)
  end

  def scan_leaf_asc(context, leaf = @root, sargs = {})
    return [] if leaf.nil?

    Enumerator.new do |y|
      lower_bound = sargs[:gt] || sargs[:gte]
      upper_bound = sargs[:lt] || sargs[:lte]
      equal_value = sargs[:gte] || sargs[:lte] || sargs[:eq]

      above_lower = lower_bound.nil? || comparator.call(leaf.key, lower_bound) > 0
      below_upper = upper_bound.nil? || comparator.call(leaf.key, upper_bound) < 0
      equal_match = equal_value && comparator.call(leaf.key, equal_value) == 0

      context.increment(:index_comparisons)

      if above_lower
        scan_leaf_asc(context, leaf.left, sargs).each do |v|
          y << v
        end
      end

      if (above_lower && below_upper && !sargs[:eq]) || equal_match
        y << leaf.value
      end

      if below_upper
        scan_leaf_asc(context, leaf.right, sargs).each do |v|
          y << v
        end
      end
    end
  end

  def scan_leaf_desc(context, leaf = @root, sargs = {})
    return [] if leaf.nil?

    Enumerator.new do |y|
      lower_bound = sargs[:gt] || sargs[:gte]
      upper_bound = sargs[:lt] || sargs[:lte]
      equal_value = sargs[:gte] || sargs[:lte] || sargs[:eq]

      above_lower = lower_bound.nil? || comparator.call(leaf.key, lower_bound) > 0
      below_upper = upper_bound.nil? || comparator.call(leaf.key, upper_bound) < 0
      equal_match = equal_value && comparator.call(leaf.key, equal_value) == 0

      context.increment(:index_comparisons)

      if below_upper
        scan_leaf_desc(context, leaf.right, sargs).each do |v|
          y << v
        end
      end

      if (above_lower && below_upper && !sargs[:eq]) || equal_match
        y << leaf.value
      end

      if above_lower
        scan_leaf_desc(context, leaf.left, sargs).each do |v|
          y << v
        end
      end
    end
  end

  # find exact match or parent node, where we could
  # insert a node this useful to find a starting point
  # for scans
  def find_insert_point(target, leaf = @root)
    @num_comparisons += 1

    return nil if leaf.nil?

    case @comparator.call(leaf.key, target)
    when -1
      find_insert_point(target, leaf.right) || leaf
    when 0
      leaf
    when 1
      find_insert_point(target, leaf.left) || leaf
    else
      raise
    end
  end

  def print_tree(leaf = @root, depth = 0)
    return unless leaf
    puts (" " * depth * 2) + leaf.key.inspect
    print_tree(leaf.left, depth + 1)
    print_tree(leaf.right, depth + 1)
  end

  private

  def invalidate_cached_values
    @min = @max = nil
  end

  def locate(target, leaf)
    @num_comparisons += 1

    return nil if leaf.nil?

    case @comparator.call(leaf.key, target)
    when -1
      locate target, leaf.right
    when 1
      locate target, leaf.left
    when 0
      leaf
    else
      raise
    end
  end

  def locate_min(leaf)
    return nil if leaf.nil?
    return leaf if leaf.left.nil?
    locate_min leaf.left
  end

  def locate_max(leaf)
    return nil if leaf.nil?
    return leaf if leaf.right.nil?
    locate_max leaf.right
  end

  def recompute_heights(start_from_node)
    changed = true
    node = start_from_node
    while node && changed
      old_height = node.height
      node.height = if node.right || node.left
                      node.max_children_height + 1
                    else
                      0
                    end
      changed = node.height != old_height
      node = node.parent
    end
  end

  def put(element, value, leaf, parent, link_type = nil)
    # once you reach a point where you can place a new node
    if leaf.nil?
      # create that new node
      leaf = BinaryNode.new element, value, parent
      @size += 1
      invalidate_cached_values
      if parent
        if 'left' == link_type
          parent.left = leaf
        else
          parent.right = leaf
        end
      else
        @root = leaf
      end
      if parent && parent.height.zero?
        # if it has a parent but it is balanced, move up
        node = parent
        node_to_rebalance = nil

        # continue moving up until you git the root
        while node
          node.height = node.max_children_height + 1
          if node.balance_factor.abs > 1
            node_to_rebalance = node
            break
          end
          node = node.parent
        end
        # if at any point you reach an unbalanced node, rebalance it
        rebalance node_to_rebalance if node_to_rebalance
      end
    else
      case @comparator.call(leaf.key, element)
      when -1
        put element, value, leaf.right, leaf, 'right'
      when 1
        put element, value, leaf.left, leaf, 'left'
      when 0
        leaf.value = value
      else
        raise
      end
    end
  end

  def find_value_ex(leaf, value)
    if leaf
      node_with_value = find_value_ex leaf.left, value
      return node_with_value if node_with_value
      return leaf if leaf.value == value
      node_with_value = find_value_ex leaf.right, value
      return node_with_value if node_with_value
    end
    nil
  end

  def serialize_nodes(leaf)
    return if leaf.nil?
    serialize_nodes leaf.left
    @nodes << leaf
    serialize_nodes leaf.right
  end

  def compare(leaf, other_bst_leaf)
    if leaf && other_bst_leaf
      leaf.value == other_bst_leaf.value &&
        compare(leaf.left, other_bst_leaf.left) &&
        compare(leaf.right, other_bst_leaf.right)
    else
      leaf.nil? && other_bst_leaf.nil?
    end
  end

  def assert(condition)
    raise 'assertion failed' unless condition
  end

  def rrc_rebalance(a, f)
    b = a.right
    c = b.right
    assert a && b && c
    a.right = b.left
    a.right.parent = a if a.right
    b.left = a
    a.parent = b
    if f.nil?
      @root = b
      @root.parent = nil
    else
      if f.right == a
        f.right = b
      else
        f.left = b
      end
      b.parent = f
    end
    recompute_heights a
    recompute_heights b.parent
  end

  def rlc_rebalance(a, f)
    b = a.right
    c = b.left
    assert a && b && c
    b.left = c.right
    b.left.parent = b if b.left
    a.right = c.left
    a.right.parent = a if a.right
    c.right = b
    b.parent = c
    c.left = a
    a.parent = c
    if f.nil?
      @root = c
      @root.parent = nil
    else
      if f.right == a
        f.right = c
      else
        f.left = c
      end
      c.parent = f
    end
    recompute_heights a
    recompute_heights b
  end

  def llc_rebalance(a, b, c, f)
    assert a && b && c
    a.left = b.right
    a.left.parent = a if a.left
    b.right = a
    a.parent = b
    if f.nil?
      @root = b
      @root.parent = nil
    else
      if f.right == a
        f.right = b
      else
        f.left = b
      end
      b.parent = f
    end
    recompute_heights a
    recompute_heights b.parent
  end

  def lrc_rebalance(a, b, c, f)
    assert a && b && c
    a.left = c.right
    a.left.parent = a if a.left
    b.right = c.left
    b.right.parent = b if b.right
    c.left = b
    b.parent = c
    c.right = a
    a.parent = c
    if f.nil?
      @root = c
      @root.parent = nil
    else
      if f.right == a
        f.right = c
      else
        f.left = c
      end
      c.parent = f
    end
    recompute_heights a
    recompute_heights b
  end

  def rebalance(node_to_rebalance)
    a = node_to_rebalance
    f = a.parent # allowed to be NULL
    if node_to_rebalance.balance_factor == -2
      if node_to_rebalance.right.balance_factor <= 0
        # """Rebalance, case RRC """
        rrc_rebalance a, f
      else
        rlc_rebalance a, f
        # """Rebalance, case RLC """
      end
    else
      assert node_to_rebalance.balance_factor == 2
      if node_to_rebalance.left.balance_factor >= 0
        b = a.left
        c = b.left
        # """Rebalance, case LLC """
        llc_rebalance a, b, c, f
      else
        b = a.left
        c = b.right
        #  """Rebalance, case LRC """
        lrc_rebalance a, b, c, f
      end
    end
  end

  def delete(node_or_key)
    node = if BinaryNode == node_or_key.class
             node_or_key
           else
             find node_or_key
           end

    if node
      @size -= 1
      invalidate_cached_values

      # There are three cases:
      #
      # 1) The node is a leaf.  Remove it and return.
      #
      # 2) The node is a branch (has only 1 child). Make the pointer to
      #    this node point to the child of this node.
      #
      # 3) The node has two children. Swap items with the successor
      #    of the node (the smallest item in its right subtree) and
      #    delete the successor from the right subtree of the node.
      if node.leaf?
        remove_leaf node
      elsif (!!node.left) ^ !!node.right
        remove_branch node
      else
        assert node.left && node.right
        swap_with_successor_and_remove node
      end
    end
    node
  end

  def remove_leaf(node)
    parent = node.parent
    if parent
      if parent.left == node
        parent.left = nil
      else
        assert parent.right == node
        parent.right = nil
      end
      recompute_heights parent
    else
      @root = nil
    end
    # del node
    # rebalance
    node = parent
    while node
      rebalance node unless [-1, 0, 1].include? node.balance_factor
      node = node.parent
    end
  end

  def remove_branch(node)
    parent = node.parent
    if parent
      if parent.left == node
        parent.left = node.right || node.left
      else
        assert parent.right == node
        parent.right = node.right || node.left
      end
      if node.left
        node.left.parent = parent
      else
        assert node.right
        node.right.parent = parent
      end
      recompute_heights parent
    else
      if node.left
        @root = node.left
        node.left.parent = nil
      else
        @root = node.right
        node.right.parent = nil
      end
      recompute_heights @root
    end

    # rebalance
    node = parent
    while node
      rebalance node unless [-1, 0, 1].include? node.balance_factor
      node = node.parent
    end
  end

  def swap_with_successor_and_remove(node)
    successor = locate_min node.right
    swap_nodes node, successor
    assert node.left.nil?
    if node.height.zero?
      remove_leaf node
    else
      remove_branch node
    end
  end

  def swap_nodes(node1, node2)
    assert node1.height > node2.height
    parent1 = node1.parent
    left_child1 = node1.left
    right_child1 = node1.right
    parent2 = node2.parent
    assert parent2
    assert parent2.left == node2 || parent2 == node1
    left_child2 = node2.left
    assert left_child2.nil?
    right_child2 = node2.right

    # swap heights
    tmp = node1.height
    node1.height = node2.height
    node2.height = tmp

    if parent1
      if parent1.left == node1
        parent1.left = node2
      else
        assert parent1.right == node1
        parent1.right = node2
      end
      node2.parent = parent1
    else
      @root = node2
      @root.parent = nil
    end

    node2.left = left_child1
    left_child1.parent = node2
    node1.left = left_child2 # None
    node1.right = right_child2

    right_child2.parent = node1 if right_child2

    if parent2 != node1
      node2.right = right_child1
      right_child1.parent = node2

      parent2.left = node1
      node1.parent = parent2
    else
      node2.right = node1
      node1.parent = node2
    end
  end

  def compute_and_check_height(root)
    return 0 if root.nil?
    left_sub_tree_height = compute_and_check_height root.left
    return -1 if -1 == left_sub_tree_height

    right_sub_tree_height = compute_and_check_height root.right
    return -1 if -1 == right_sub_tree_height

    height_difference = (left_sub_tree_height - right_sub_tree_height).abs

    if height_difference > 1
      -1
    else
      [left_sub_tree_height, right_sub_tree_height].max + 1
    end
  end
end

class BinaryNode
  attr_accessor :height, :parent, :left, :right, :key, :value

  def initialize(key, value, parent)
    @key = key
    @value = value
    @parent = parent
    @height = 0
  end

  def leaf?
    height.zero?
  end
  alias is_leaf? leaf?

  def max_children_height
    if left && right
      [left.height, right.height].max
    elsif left
      left.height
    elsif right
      right.height
    else
      -1
    end
  end

  def balance_factor
    left_height = if left
                    left.height
                  else
                    -1
                  end

    right_height = if right
                     right.height
                   else
                     -1
                   end

    left_height - right_height
  end
end
