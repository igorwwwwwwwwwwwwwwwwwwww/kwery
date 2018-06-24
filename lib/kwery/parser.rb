require 'rly'

module Kwery
  class Parser < Rly::Yacc
    def initialize(options = {})
      super(Kwery::Parser::Lexer.new)
      @options = options
      @anon_fields = 0
    end

    rule 'query : select_query
                | EXPLAIN select_query' do |st, e1, e2|
      st.value = (e2 || e1).value
      st.value.options[:explain] = true if e2
    end

    rule 'select_query : select_clause from_clause where_clause order_by_clause limit_clause
        ' do |st, e1, e2, e3, e4, e5|
      args = []
      args << e1.value
      args << e2.value if e2
      args << e3.value if e3
      args << e4.value if e4
      args << e5.value if e5

      args = args.compact.to_h
      args[:select_star] = args[:select].delete(:*) != nil
      args[:options] = @options

      # TODO: normalize top-level :where ANDs to be more optimizer-friendly

      st.value = Kwery::Query.new(**args)
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

    rule 'where_exprs : where_expr' do |st, e1, _, e2|
      st.value = [e1.value]
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

    rule 'exprs : expr
                | expr "," exprs' do |st, e1, _, e2|
      st.value = []
      st.value << e1.value
      st.value += e2.value if e2
    end

    rule 'expr : value
               | expr COMPARE expr' do |st, e1, c, e2|
      if e2
        case c.value
        when '='
          st.value = Kwery::Expr::Eq.new(e1.value, e2.value)
        when '<'
          st.value = Kwery::Expr::Lt.new(e1.value, e2.value)
        when '>'
          st.value = Kwery::Expr::Gt.new(e1.value, e2.value)
        when '<='
          st.value = Kwery::Expr::Lte.new(e1.value, e2.value)
        when '>='
          st.value = Kwery::Expr::Gte.new(e1.value, e2.value)
        when '<>'
          raise NotImplementedError
        end
      else
        st.value = e1.value
      end
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
