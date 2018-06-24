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

    rule 'select_query : SELECT select_expr
                       | SELECT select_expr FROM ID
                       | SELECT select_expr WHERE where_expr
                       | SELECT select_expr FROM ID WHERE where_expr' do |st, _, e1, t2, e2, t3, e3|
      args = {}
      args[:select_star] = e1.value.delete(:*) != nil
      args[:select] = e1.value
      args[t2.type.downcase] = e2.value if e2
      args[t3.type.downcase] = e3.value if e3
      args[:options] = @options

      # TODO: normalize top-level ANDs to be more optimizer-friendly
      args[:where] = [args[:where]] if args[:where]

      st.value = Kwery::Query.new(**args)
    end

    rule 'select_expr : expr
                      | expr AS ID
                      | select_expr "," select_expr' do |st, e1, _, e2|
      if e2&.type == :select_expr
        st.value ||= {}
        st.value.merge!(e1.value)
        st.value.merge!(e2.value)
        next
      end

      if e2&.type == :ID
        field_alias = e2.value
        st.value ||= {}
        st.value[field_alias] = e1.value
        next
      end

      if Kwery::Expr::Column === e1.value
        field_alias = e1.value.name
        st.value ||= {}
        st.value[field_alias] = e1.value
        next
      end

      field_alias = "_#{@anon_fields}".to_sym
      @anon_fields += 1
      st.value ||= {}
      st.value[field_alias] = e1.value
    end

    rule 'where_expr : expr
                     | where_expr AND where_expr
                     | where_expr OR where_expr' do |st, e1, op, e2|
      if op
        if op.type == :AND
          st.value = Kwery::Expr::And.new(e1.value, e2.value)
        else
          st.value = Kwery::Expr::Or.new(e1.value, e2.value)
        end
        next
      end

      st.value = e1.value
    end

    rule 'column : ID' do |st, e1|
      st.value = Kwery::Expr::Column.new(e1.value)
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
