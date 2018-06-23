require 'rly'

module Kwery
  class Parser
    class Lexer < Rly::Lex
      ignore " \t\n"

      token :EXPLAIN, /EXPLAIN/i
      token :SELECT, /SELECT/i
      token :FROM, /FROM/i
      token :WHERE, /WHERE/i
      token :AS, /AS/i

      token :NUMBER, /\d+/ do |t|
        t.value = t.value.to_i
        t
      end

      token :BOOL, /(true|false|t|f)/ do |t|
        t.value = ['true', 't'].include?(t.value)
        t
      end

      token :STRING, /(?:'(?<val>[^'\\]*(?:\\.[^'\\]*)*)')/ do |t|
        t.value = t.value[1..-2].gsub("\\'", "'")
        t
      end

      token :COMPARE, /(=|<|>|<=|>=|<>)/

      token :ID, /[a-zA-Z*]+/ do |t|
        t.value = t.value.to_sym
        t
      end

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
