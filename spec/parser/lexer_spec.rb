require 'kwery'

RSpec.describe Kwery::Parser::Lexer do
  it "lexes select 1" do
    tokens =
    sql = 'SELECT 1'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 1],
    ])
  end
end
