require 'rly'

module Kwery
  class Parser
    class Parser < Rly::Yacc
      def initialize
        super(Kwery::Parser::Lexer.new)
        @anon_fields = 0
      end

      rule 'query : SELECT fields
                  | SELECT fields FROM ID
                  | SELECT fields FROM ID WHERE expr' do |st, _, e1, t2, e2, t3, e3|
        args = {}
        args[:select] = e1.value
        args[t2.type.downcase] = e2.value if e2
        args[t3.type.downcase] = e3.value if e3

        st.value = Kwery::Query.new(**args)
      end

      rule 'fields : expr
                   | expr AS ID' do |st, e1, _, e2|
        field_alias = e2&.value || e1.value
        if e2
          field_alias = e2.value
        elsif e1.value.is_a?(Kwery::Expr::Column)
          field_alias = e1.value.name
        else
          field_alias = "_#{@anon_fields}".to_sym
          @anon_fields += 1
        end
        st.value = {field_alias => e1.value}
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
          location_info = lookahead.location_info
          raise "Syntax error at #{location_info}, token='#{errtoken}'"
        else
          raise "Parse error in input. EOF"
        end
      }
    end
  end
end
