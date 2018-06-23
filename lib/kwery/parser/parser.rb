require 'rly'

module Kwery
  class Parser
    class Parser < Rly::Yacc
      def initialize
        super(Kwery::Parser::Lexer.new)
      end

      rule 'query : SELECT expr
                  | SELECT expr FROM ID
                  | SELECT expr FROM ID WHERE expr' do |st, _, e1, t2, e2, t3, e3|
        args = {}
        args[:select] = e1.value
        args[t2.type.downcase] = e2.value if e2
        args[t3.type.downcase] = e3.value if e3

        args[:from] = args[:from].to_sym if args[:from]

        st.value = Kwery::Query.new(**args)
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
          st.value = Kwery::Expr::Column.new(e1.value.to_sym)
        end
      end
    end
  end
end
