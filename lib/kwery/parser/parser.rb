require 'rly'

module Kwery
  class Parser
    class Parser < Rly::Yacc
      def initialize
        super(Kwery::Parser::Lexer.new)
      end

      rule 'program : SELECT expr
                    | SELECT expr FROM ID
                    | SELECT expr FROM ID WHERE expr' do |st, _, e1, t2, e2, t3, e3|
        st.value = [:select, e1.value]
        st.value << [t2.type.downcase, e2.value] if e2
        st.value << [t3.type.downcase, e3.value] if e3
      end

      rule 'expr : value
                 | expr COMPARE expr' do |st, e1, c, e2|
        if e2
          st.value = [c.value, e1.value, e2.value]
        else
          st.value = e1.value
        end
      end

      rule 'value : NUMBER
                  | STRING
                  | BOOL
                  | ID
                  | STAR' do |st, e1|
        if [:ID, :STAR].include?(e1.type)
          st.value = [:id, e1.value]
        else
          st.value = [:value, e1.value]
        end
      end
    end
  end
end
