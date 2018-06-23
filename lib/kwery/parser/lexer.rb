require 'rly'

module Kwery
  class Parser
    class Lexer < Rly::Lex
      ignore " \t\n"

      token :SELECT, /SELECT/i
      token :FROM, /FROM/i
      token :WHERE, /WHERE/i

      token :NUMBER, /\d+/ do |t|
        t.value = t.value.to_i
        t
      end

      token :BOOL, /(true|false)/ do |t|
        t.value = t.value == 'true'
        t
      end

      token :STRING, /(?:'(?<val>[^'\\]*(?:\\.[^'\\]*)*)')/ do |t|
        t.value = t.value[1..-2].gsub("\\'", "'")
        t
      end

      token :STAR, /\*/
      token :COMPARE, /(=|<|>|<=|>=|<>)/
      token :ID, /[a-zA-Z*]+/

      def to_enum
        Enumerator.new do |g|
          while token = self.next
            g.yield token
          end
        end
      end

      def pairs
        to_enum.map { |token| [token.type, token.value] }
      end
    end
  end
end
