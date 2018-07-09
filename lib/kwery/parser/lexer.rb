require 'rly'

module Kwery
  class Parser
    class Lexer < Rly::Lex
      ignore " \t\n"
      literals ',()'

      token :EXPLAIN, /\bEXPLAIN\b/i
      token :SELECT, /\bSELECT\b/i
      token :AS, /\bAS\b/i
      token :FROM, /\bFROM\b/i
      token :WHERE, /\bWHERE\b/i
      token :AND, /\bAND\b/i
      token :OR, /\bOR\b/i
      token :IN, /\bIN\b/i
      token :ORDER_BY, /\bORDER BY\b/i
      token :GROUP_BY, /\bGROUP BY\b/i
      token :ASC_DESC, /\b(ASC|DESC)\b/i
      token :LIMIT, /\bLIMIT\b/i

      token :INSERT, /\bINSERT\b/i
      token :INTO, /\bINTO\b/i
      token :VALUES, /\bVALUES\b/i

      token :UPDATE, /\bUPDATE\b/i
      token :SET, /\bSET\b/i

      token :DELETE, /\bDELETE\b/i

      token :COPY, /\bCOPY\b/i

      token :RESHARD, /\bRESHARD\b/i
      token :MOVE, /\bMOVE\b/i
      token :TO, /\bTO\b/i
      token :RECEIVE, /\bRECEIVE\b/i

      token :NUMBER, /\d+/ do |t|
        t.value = t.value.to_i
        t
      end

      token :BOOL, /\b(true|false|t|f)\b/ do |t|
        t.value = ['true', 't'].include?(t.value)
        t
      end

      token :STRING, /(?:'(?<val>[^'\\]*(?:\\.[^'\\]*)*)')/ do |t|
        t.value = t.value[1..-2].gsub("\\'", "'")
        t
      end

      token :COMPARE, /(<=|>=|<>|!=|<|>)/
      token :EQ, /(=)/

      token :ID, /(\b[a-zA-Z_][a-zA-Z0-9_]*\b|\*)/ do |t|
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
