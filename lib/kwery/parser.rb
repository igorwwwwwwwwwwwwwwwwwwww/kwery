require 'rly'

module Kwery
  class Parser < Rly::Yacc
    def initialize
      super(Kwery::Parser::Lexer.new)
    end

    def parse(*args)
      @anon_fields = 0
      super
    end

    def normalize_where(node)
      exprs = []

      if Kwery::Expr::And === node
        exprs += normalize_where(node.left)
        exprs += normalize_where(node.right)
      else
        exprs << node
      end

      exprs
    end

    rule 'query : explainable_query
                | EXPLAIN explainable_query' do |st, e1, e2|
      st.value = (e2 || e1).value
      st.value.options[:explain] = true if e2
    end

    rule 'explainable_query : select_query
                            | insert_query
                            | update_query
                            | delete_query
                            | copy_query
                            | reshard_query' do |st, e1|
      st.value = e1.value
    end

    rule 'select_query : select_clause from_clause where_clause group_by_clause order_by_clause limit_clause
        ' do |st, e1, e2, e3, e4, e5, e6|
      args = []
      args << e1.value
      args << e2.value if e2
      args << e3.value if e3
      args << e4.value if e4
      args << e5.value if e5
      args << e6.value if e6

      args = args.compact.to_h
      args[:options] = {}

      st.value = Kwery::Query::Select.new(**args)
    end

    rule 'select_clause : SELECT select_exprs' do |st, _, e1|
      st.value = [:select, e1.value.to_h]
    end

    rule 'select_exprs : select_expr
                       | select_expr "," select_exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'select_expr : expr
                      | expr AS ID' do |st, e1, _, e2|
      if e2
        field_alias = e2.value
      elsif Kwery::Expr::Column === e1.value
        field_alias = e1.value.name
      else
        field_alias = "_#{@anon_fields}".to_sym
        @anon_fields += 1
      end

      st.value = [field_alias, e1.value]
    end

    rule 'from_clause : FROM ID
                      |' do |st, _, e1|
      st.value = [:from, e1.value] if e1
    end

    rule 'where_clause : WHERE where_exprs
                       |' do |st, _, e1|
      st.value = [:where, e1.value] if e1
    end

    rule 'where_exprs : where_expr' do |st, e1|
      st.value = normalize_where(e1.value)
    end

    rule 'where_expr : expr
                     | where_expr AND where_expr
                     | where_expr OR where_expr
                     | expr IN "(" exprs ")"' do |st, e1, op, e2, e3|
      if op
        case op.type
        when :AND
          st.value = Kwery::Expr::And.new(e1.value, e2.value)
        when :OR
          st.value = Kwery::Expr::Or.new(e1.value, e2.value)
        when :IN
          st.value = Kwery::Expr::In.new(e1.value, e3.value)
        end
        next
      end
      st.value = e1.value
    end

    rule 'group_by_clause : GROUP_BY group_by_exprs
                          |' do |st, _, e1|
      st.value = [:group_by, e1.value] if e1
    end

    rule 'group_by_exprs : expr
                         | expr "," exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'order_by_clause : ORDER_BY order_by_exprs
                          |' do |st, _, e1|
      st.value = [:order_by, e1.value] if e1
    end

    rule 'order_by_exprs : order_by_expr
                         | order_by_expr "," order_by_exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'order_by_expr : expr
                        | expr ASC_DESC' do |st, e1, e2|
      order = :asc
      order = e2.value.to_sym.downcase if e2
      st.value = Kwery::Expr::IndexedExpr.new(e1.value, order)
    end

    rule 'limit_clause : LIMIT NUMBER
                       |' do |st, _, e1|
      st.value = [:limit, e1.value] if e1
    end

    rule 'insert_query : INSERT INTO ID "(" ids ")" VALUES insert_exprs' do |st, _, _, e1, _, e2, _, _, e3|
      args = {}
      args[:into]   = e1.value
      args[:keys]   = e2.value
      args[:values] = e3.value

      st.value = Kwery::Query::Insert.new(**args)
    end

    rule 'insert_exprs : insert_expr
                       | insert_expr "," insert_exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'insert_expr : "(" exprs ")"' do |st, _, e1|
      st.value = e1.value
    end

    rule 'update_query : UPDATE ID SET update_exprs
                       | UPDATE ID SET update_exprs WHERE where_exprs' do |st, _, e1, _, e2, _, e3|
      args = {}
      args[:table]  = e1.value
      args[:update] = e2.value
      args[:where]  = e3.value if e3

      st.value = Kwery::Query::Update.new(**args)
    end

    rule 'update_exprs : update_expr
                       | update_expr "," update_exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'update_expr : ID EQ expr' do |st, e1, _, e2|
      st.value = [e1.value, e2.value]
    end

    rule 'delete_query : DELETE FROM ID
                       | DELETE FROM ID WHERE where_exprs' do |st, _, _, e1, _, e2|
      args = {}
      args[:from]  = e1.value
      args[:where] = e2.value if e2

      st.value = Kwery::Query::Delete.new(**args)
    end

    rule 'copy_query : COPY ID FROM ID
                     | COPY ID FROM STRING' do |st, _, e1, _, e2|
      args = {}
      args[:table] = e1.value
      args[:from]  = e2.value

      st.value = Kwery::Query::Copy.new(**args)
    end

    rule 'reshard_query : reshard_move_query
                        | reshard_receive_query' do |st, e1|
      st.value = e1.value
    end

    rule 'reshard_move_query : RESHARD ID MOVE NUMBER TO STRING' do |st, _, e1, _, e2, _, e3|
      args = {}
      args[:table]  = e1.value
      args[:shard]  = e2.value
      args[:target] = e3.value

      st.value = Kwery::Query::ReshardMove.new(**args)
    end

    rule 'reshard_receive_query : RESHARD ID RECEIVE NUMBER' do |st, _, e1, _, e2|
      args = {}
      args[:table] = e1.value
      args[:shard] = e2.value

      st.value = Kwery::Query::ReshardReceive.new(**args)
    end

    rule 'ids : ID
              | ID "," ids' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'exprs : expr
                | expr "," exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'expr : value
               | expr COMPARE expr
               | expr EQ expr
               | ID "(" exprs ")"' do |st, e1, e2, e3|
      if e2&.type == :COMPARE || e2&.type == :EQ
        case e2.value
        when '='
          st.value = Kwery::Expr::Eq.new(e1.value, e3.value)
        when '<'
          st.value = Kwery::Expr::Lt.new(e1.value, e3.value)
        when '>'
          st.value = Kwery::Expr::Gt.new(e1.value, e3.value)
        when '<='
          st.value = Kwery::Expr::Lte.new(e1.value, e3.value)
        when '>='
          st.value = Kwery::Expr::Gte.new(e1.value, e3.value)
        when '<>'
          st.value = Kwery::Expr::Neq.new(e1.value, e3.value)
        when '!='
          st.value = Kwery::Expr::Neq.new(e1.value, e3.value)
        end
        next
      end

      if e1&.type == :ID
        st.value = Kwery::Expr::FnCall.new(e1.value, e3.value)
        next
      end

      st.value = e1.value
    end

    rule 'value : NUMBER
                | STRING
                | BOOL
                | ID' do |st, e1|
      case e1.type
      when :NUMBER
        st.value = Kwery::Expr::Literal.new(e1.value)
      when :STRING
        st.value = Kwery::Expr::Literal.new(e1.value)
      when :BOOL
        st.value = Kwery::Expr::Literal.new(e1.value)
      when :ID
        st.value = Kwery::Expr::Column.new(e1.value)
      end
    end

    on_error lambda { |errtoken|
      if errtoken
        raise "Syntax error at #{errtoken.location_info}, token='#{errtoken}'"
      else
        raise "Parse error in input. EOF"
      end
    }
  end
end
